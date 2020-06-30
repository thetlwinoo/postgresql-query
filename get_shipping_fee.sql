CREATE OR REPLACE FUNCTION get_shipping_fee(_cart_id bigint,_supplier_id bigint,_delivery_method_id bigint) RETURNS NUMERIC(21,2) AS $$
DECLARE v_shipping_fee NUMERIC(21,2);
DECLARE v_dest_zone_id BIGINT;

BEGIN
	SELECT INTO v_dest_zone_id ad.zone_id
	FROM customers c 
	INNER JOIN shopping_carts sc
	ON sc.customer_id = c.id
	INNER JOIN addresses ad
	ON c.delivery_address_id = ad.id
	WHERE sc.id = _cart_id;
		
	SELECT INTO v_shipping_fee
	(
		COALESCE(
		(SELECT MAX(sfc.price)::NUMERIC(21,2)
		FROM shipping_fee_chart sfc
		INNER JOIN delivery_methods dm
		ON sfc.delivery_method_id = dm.id
		WHERE sfc.destination_zone_id = v_dest_zone_id
		AND sfc.source_zone_id = (SELECT ad.zone_id FROM suppliers sp INNER JOIN addresses ad ON sp.pickup_address_id = ad.id WHERE sp.id = _supplier_id)
		AND sfc.delivery_method_id = _delivery_method_id
		AND	(
			(SUM((si.item_length + si.item_width + si.item_height) * sci.quantity)) BETWEEN sfc.min_volume_weight AND sfc.max_volume_weight
				OR
			(SUM((si.item_length + si.item_width + si.item_height) * sci.quantity)) > sfc.max_volume_weight
			)
		),0)
	) AS shipping_fee
	FROM shopping_carts sc
	INNER JOIN shopping_cart_items sci
	ON sci.cart_id = sc.id
	INNER JOIN stock_items si
	ON sci.stock_item_id = si.id
	INNER JOIN products p
	ON si.product_id = p.id
	INNER JOIN suppliers s
	ON p.supplier_id = s.id
	WHERE sc.id = _cart_id
	AND sci.select_order = true
	AND p.supplier_id = _supplier_id;
	
	RETURN v_shipping_fee;
END;
$$ LANGUAGE plpgsql;




