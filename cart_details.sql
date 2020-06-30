-- DROP FUNCTION cart_details(bigint);

CREATE OR REPLACE FUNCTION cart_details(_shopping_cart_id bigint) RETURNS TEXT AS $$	
	DECLARE v_return text;
	DECLARE v_total_quantity INTEGER;
	DECLARE v_item_count INTEGER;
	DECLARE v_overall_check_status INTEGER;
	
	BEGIN
		SELECT INTO v_total_quantity SUM(sci.quantity)
		FROM shopping_cart_items sci
		INNER JOIN shopping_carts sc
		ON sci.cart_id = sc.id
		WHERE sc.id = _shopping_cart_id;

		SELECT INTO v_item_count COUNT(*)
		FROM shopping_cart_items sci
		INNER JOIN shopping_carts sc
		ON sci.cart_id = sc.id
		WHERE sc.id = _shopping_cart_id;
				
		SELECT INTO v_overall_check_status 
			(CASE 
				WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) = COUNT(sci.*) THEN 1
				WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) = 0 THEN 0
				WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) < COUNT(sci.*) THEN 2
			END)
		FROM shopping_cart_items sci
		INNER JOIN shopping_carts sc
		ON sci.cart_id = sc.id
		WHERE sc.id = _shopping_cart_id;
				
		SELECT INTO v_return 		
		(
			SELECT to_json(
				json_build_object(
					'cartPrice',cart_details.cart_price,
					'totalQuantity',cart_details.total_quantity,
					'itemCount',cart_details.item_count,
					'selectedCount',cart_details.selected_count,
					'selectedQuantity',cart_details.selected_quantity,
					'stockItemList',cart_details.stock_item_list,
					'cartPackages',cart_details.cart_packages,
					'checkStatus',cart_details.check_status
				)
			)
			FROM
			(SELECT 
				COALESCE(SUM(sci3.quantity * si3.unit_price),0) AS cart_price,
				v_total_quantity AS total_quantity,
				v_item_count AS item_count,
			 	COUNT(sci3.*) AS selected_count,
				COALESCE(SUM(sci3.quantity),0) AS selected_quantity,			 	
				array_to_string(ARRAY(
					SELECT si4.id
					FROM shopping_carts sc4
					INNER JOIN shopping_cart_items sci4
					ON sc4.id = sci4.cart_id
					INNER JOIN stock_items si4
					ON sci4.stock_item_id = si4.id
					WHERE sc4.id = _shopping_cart_id
-- 					AND sci4.select_order = true
				), ',', '*') AS stock_item_list,
				(
					SELECT json_agg(
						json_build_object(
							'supplierId',supplier_groups.supplier_id,
							'supplierName',supplier_groups.supplier_name,
							'checkStatus',supplier_groups.check_status,
							'cartItems',supplier_groups.cart_items
						)
					) AS cart_packages
					FROM
					(
						SELECT 
							sp.id AS supplier_id,
							sp.name AS supplier_name,
					 	CASE 
					 		WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) = COUNT(sci.*) THEN 1
					 		WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) = 0 THEN 0
					 		WHEN COALESCE((SUM(CASE WHEN sci.select_order = true THEN 1 ELSE 0 END)),0) < COUNT(sci.*) THEN 2
					 	END AS check_status,
						(
							SELECT json_agg(
								json_build_object(
									'cartItemId',details.cart_item_id,
									'selectOrder',details.select_order,
									'stockItemId',details.stock_item_id,
									'stockItemName',details.stock_item_name,
									'productId',details.product_id,
									'productName',details.product_name,
									'supplierId',details.supplier_id,
									'supplierName',details.supplier_name,
									'quantity',details.quantity,
									'unitPrice',details.unit_price,
									'quantityOnHand',details.quantity_on_hand,
									'thumbnailUrl',details.thumbnail_url
								)
							) AS cart_items
							FROM
								(SELECT 
									sci2.id AS cart_item_id,
									sci2.select_order AS select_order,
									si2.id AS stock_item_id,
									si2.name AS stock_item_name,
									pd2.id AS product_id,
									pd2.name AS product_name,
									sp2.id AS supplier_id,
									sp2.name AS supplier_name,
									sci2.quantity,
									si2.unit_price,
									si2.quantity_on_hand,
									si2.thumbnail_url
								FROM shopping_carts sc2
								INNER JOIN shopping_cart_items sci2
								ON sc2.id = sci2.cart_id
								INNER JOIN stock_items si2
								ON sci2.stock_item_id = si2.id
								INNER JOIN products pd2
								ON si2.product_id = pd2.id
								INNER JOIN suppliers sp2
								ON sp2.id = pd2.supplier_id
								WHERE sp2.id = sp.id
								) AS details
						)
					FROM shopping_carts sc
					INNER JOIN shopping_cart_items sci
					ON sc.id = sci.cart_id
					INNER JOIN stock_items si
					ON sci.stock_item_id = si.id
					INNER JOIN products pd
					ON si.product_id = pd.id
					INNER JOIN suppliers sp
					ON sp.id = pd.supplier_id
					GROUP BY sp.id,sp.name) supplier_groups
				),			 	
			v_overall_check_status AS check_status
			FROM shopping_carts sc3
			INNER JOIN shopping_cart_items sci3
			ON sc3.id = sci3.cart_id
			INNER JOIN stock_items si3
			ON sci3.stock_item_id = si3.id
			WHERE sc3.id = _shopping_cart_id
			AND sci3.select_order = true) AS cart_details
		);

    RETURN v_return;
	END;
	
$$ LANGUAGE plpgsql;