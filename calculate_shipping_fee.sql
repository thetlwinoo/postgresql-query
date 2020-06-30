-- DROP FUNCTION calculate_shipping_fee(bigint);

CREATE OR REPLACE FUNCTION calculate_shipping_fee(_shopping_cart_id bigint) 
	RETURNS 
		TABLE (
			supplier_id BIGINT,
			package_name VARCHAR(255),
			volume_weight INTEGER,
			actual_weight NUMERIC(21,2),
			total_quantity INTEGER,
			total_items INTEGER,
			shipping_fee NUMERIC(21,2),
			total_price NUMERIC(21,2),
			shopping_items TEXT,
			suppliers_delivery_method TEXT
		) 
	AS $$
	
	DECLARE v_return text;
	DECLARE v_calculated_price numeric(21,2);
	DECLARE v_volume_weight integer;
	DECLARE v_actual_weight numeric(21,2);
	DECLARE l_context text;
	
	BEGIN				
-- 		RAISE NOTICE 'Delivery Method Id: %', v_delivery_method_id; 
	
		RETURN QUERY
		SELECT 	
		s.id::BIGINT AS supplier_id,
		s.name::VARCHAR(255) AS package_name,
		SUM((si.item_length + si.item_width + si.item_height) * sci.quantity)::INTEGER AS volume_weight,
		SUM((si.item_weight) * sci.quantity)::NUMERIC(21,2) AS actual_weight,
		SUM(sci.quantity)::INTEGER AS total_quantity,
		COUNT(sci.*)::INTEGER AS total_items,
		get_shipping_fee(_shopping_cart_id,p.supplier_id,MIN(sci.delivery_method_id)) AS shipping_fee,
		SUM(si.unit_price * sci.quantity) AS total_price,
		(
			SELECT json_agg(
				json_build_object(
					'id',shipping_items.id,
					'productId',shipping_items.product_id,
					'productName',shipping_items.product_name,
					'stockItemId',shipping_items.stock_item_id,
					'stockItemName',shipping_items.stock_item_name,
					'selectOrder',shipping_items.select_order,
					'quantity',shipping_items.quantity,
					'unitPrice',shipping_items.unit_price,
					'quantityOnHand',shipping_items.quantity_on_hand,
					'deliveryMethodId',shipping_items.delivery_method_id,
					'blobId',shipping_items.blob_id
				))::TEXT
			FROM
			(
				SELECT 
					sci2.id,
					p2.id AS product_id,
					p2.name AS product_name,
					si2.id AS stock_item_id,
					si2.name AS stock_item_name,
					sci2.select_order AS select_order,
					sci2.quantity AS quantity,
					si2.unit_price AS unit_price,
					si2.quantity_on_hand AS quantity_on_hand,
					sci2.delivery_method_id AS delivery_method_id,
					p.blob_id AS blob_id
				FROM shopping_cart_items sci2
				INNER JOIN shopping_carts sc2
				ON sci2.cart_id = sc2.id
				INNER JOIN stock_items si2
				ON sci2.stock_item_id = si2.id
				INNER JOIN photos p
				ON p.stock_item_id = si2.id
				INNER JOIN products p2
				ON si2.product_id = p2.id
				INNER JOIN suppliers s2
				ON p2.supplier_id = s2.id
				WHERE sc2.id = _shopping_cart_id						
				AND sci2.select_order = true
				AND s2.id = s.id
			) AS shipping_items
		),
		(
			SELECT json_agg(
				json_build_object(
					'deliveryMethodId',sdm_data.delivery_method_id,
					'deliveryMethodName',sdm_data.delivery_method_name,
					'thirdPartyName',sdm_data.third_party_name,
					'expectedMinArrivalDays',sdm_data.expected_min_arrival_days,
					'expectedMaxArrivalDays',sdm_data.expected_max_arrival_days,
					'deliveryNote',sdm_data.delivery_note,
					'shippingFee',sdm_data.shipping_fee
				)
			)::TEXT
			FROM
				(
					SELECT 
						dm.id as delivery_method_id,
						dm.name as delivery_method_name,
						dm.third_party_name,
						dm.expected_min_arrival_days,
						dm.expected_max_arrival_days,
						dm.delivery_note,
						get_shipping_fee(_shopping_cart_id,sdm.suppliers_id,sdm.delivery_method_id) AS shipping_fee
					FROM suppliers sp
					INNER JOIN suppliers_delivery_method sdm
					ON sp.id = sdm.suppliers_id
					INNER JOIN delivery_methods dm
					ON dm.id = sdm.delivery_method_id
					WHERE sp.id = s.id
					AND dm.active_ind = true
				) AS sdm_data
		)
		FROM shopping_carts sc
		INNER JOIN shopping_cart_items sci
		ON sci.cart_id = sc.id
		INNER JOIN stock_items si
		ON sci.stock_item_id = si.id
		INNER JOIN products p
		ON si.product_id = p.id
		INNER JOIN suppliers s
		ON p.supplier_id = s.id
		WHERE sc.id = _shopping_cart_id
		AND sci.select_order = true
		GROUP BY p.supplier_id,s.id,s.name;
	END;
	
$$ LANGUAGE plpgsql;