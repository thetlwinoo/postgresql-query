-- DROP PROCEDURE invoice_customer_orders(bigint,bigint,bigint,character varying);

CREATE OR REPLACE PROCEDURE invoice_customer_orders(
	_order_id IN BIGINT,
	_packed_by_person_id IN BIGINT,
	_invoiced_by_person_id IN BIGINT,
	_msg_error INOUT VARCHAR 
) 
LANGUAGE plpgsql 
AS $$
DECLARE
  l_context text;
  l_itg text;
BEGIN	
  CREATE TEMPORARY TABLE invoices_to_generate (
	  order_id BIGINT NOT NULL,
	  order_package_id BIGINT NOT NULL,
	  invoice_id BIGINT NOT NULL,
	  total_dry_items INT NOT NULL,
	  total_chiller_items INT NOT NULL
  ) ON COMMIT DROP;
  
  --Invoice To Generate
  INSERT INTO invoices_to_generate
  SELECT 
  	o.id, 
	ip.id,
	nextval('sequence_generator'),
	COALESCE((SELECT SUM(CASE WHEN si.is_chiller_stock <> false THEN 0 ELSE 1 END)
			 FROM order_lines AS ol
			 INNER JOIN stock_items AS si
			 ON ol.stock_item_id = si.id
			 WHERE ol.order_package_id = ip.id), 0),
	COALESCE((SELECT SUM(CASE WHEN si.is_chiller_stock <> false THEN 1 ELSE 0 END)
			 FROM order_lines AS ol
			 INNER JOIN stock_items AS si
			 ON ol.stock_item_id = si.id
			 WHERE ol.order_package_id = ip.id), 0)			 
  FROM orders o
  INNER JOIN order_packages ip
  ON o.id = ip.order_id
  WHERE o.id = _order_id
  AND NOT EXISTS ( SELECT 1 FROM invoices i WHERE i.order_id = o.id AND i.order_package_id = ip.id);
--   AND A.picking_completed_on IS NOT NULL;
  
  IF NOT EXISTS(SELECT 1 FROM invoices_to_generate AS itg WHERE itg.order_id = _order_id) THEN
  	RAISE NOTICE 'At least one order ID either does not exist, is not picked, or is already invoiced';
	RAISE EXCEPTION 'At least one order ID either does not exist, is not picked, or is already invoiced'; 
  END IF;
  
--   SELECT INTO l_itg json_agg(itg_json)
--   FROM (
--   			SELECT *
-- 			FROM invoices_to_generate
--   ) as itg_json;
  
--   RAISE NOTICE 'Invoice To Generate: %',l_itg; 
  
  --Invoices
  INSERT INTO invoices(
		id,
		invoice_date,customer_purchase_order_number,is_credit_note,credit_note_reason,comments,delivery_instructions,internal_comments,total_dry_items,total_chiller_items,delivery_run,run_position,returned_delivery_data,confirmed_delivery_time,confirmed_received_by,status,last_edited_by,last_edited_when,
		contact_person_id,
		sales_person_id,
		packed_by_person_id,
		accounts_person_id,
		customer_id,
		bill_to_customer_id,
		delivery_method_id,
		order_id,
		order_package_id,
		payment_method_id)
  SELECT 
  	itg.invoice_id,
	NOW(),
	o.customer_purchase_order_number,
	false,
	null,
	null,
	CONCAT(ads.address_line_1,', ',ads.address_line_2),
	null,
	itg.total_dry_items,
	itg.total_chiller_items,
	c.delivery_run,
	c.run_position,
	null,
	null,
	null,
	'PENDING',
	_invoiced_by_person_id,
	NOW(),
	c.people_id,
	_invoiced_by_person_id,
	_packed_by_person_id,
	_invoiced_by_person_id,
	c.id,
	c.id,
	op.delivery_method_id,
	itg.order_id,
	itg.order_package_id,
	o.payment_method_id	
  FROM invoices_to_generate itg
  INNER JOIN orders o
  ON o.id = itg.order_id
  INNER JOIN order_packages op
  ON (op.order_id = o.id AND op.id = itg.order_package_id)
  INNER JOIN customers c
  ON o.customer_id = c.id
  INNER JOIN addresses ads
  ON ads.id = c.delivery_address_id; 
  
  --Invoice Lines
  INSERT INTO invoice_lines(
	id,
	invoice_id,
	stock_item_id,
	description,
	package_type_id,
	quantity,
	unit_price,
	tax_rate,
	tax_amount,
	line_profit,
	extended_price,
	last_edited_by,
	last_edited_when
  )
  SELECT 
  	nextval('sequence_generator'),
  	itg.invoice_id,
	ol.stock_item_id,
	ol.description,
	ol.package_type_id,
	ol.quantity,
	ol.unit_price,
	ol.tax_rate,
	ROUND(ol.quantity * ol.unit_price * ol.tax_rate / 100.0,2),
	ROUND(ol.quantity * (ol.unit_price - si.last_cost_price),2),
	ROUND(ol.quantity * ol.unit_price,2) + ROUND(ol.quantity * ol.unit_price * ol.tax_rate / 100.0,2),
	_invoiced_by_person_id,
	NOW()
  FROM invoices_to_generate itg
  INNER JOIN order_lines ol
  ON itg.order_package_id = ol.order_package_id
  INNER JOIN stock_items si
  ON ol.stock_item_id = si.id
  ORDER BY ol.order_package_id,ol.id;
	
  --Stock Items Transaction
  INSERT INTO stock_item_transactions(
	  id,
	  transaction_occured_when,
	  quantity,
	  last_edited_by,
	  last_edited_when,
	  stock_item_id,
	  customer_id,
	  invoice_id,
	  supplier_id,
	  transaction_type_id,
	  purchase_order_id
  )
  SELECT 
  	nextval('sequence_generator'),
  	NOW(),
	0 - il.quantity,
	_invoiced_by_person_id,
	NOW(),
	il.stock_item_id,
	i.customer_id,
	i.id,	
  	NULL,
	(SELECT id FROM transaction_types WHERE name = N'Stock Issue'),			
	NULL		
  FROM invoices_to_generate itg
  INNER JOIN invoice_lines il
  ON itg.invoice_id = il.invoice_id
  INNER JOIN invoices i
  ON il.invoice_id = i.id
  ORDER BY il.invoice_id, il.id;
  
  --UPDATE StockItemHoldings
  WITH stock_item_totals
  AS(
		SELECT 
	  		il.stock_item_id,
	  		SUM(il.quantity) AS total_quantity
		FROM invoice_lines il
		WHERE il.invoice_id IN (SELECT invoice_id FROM invoices_to_generate)
	  	GROUP BY il.stock_item_id
  )
  UPDATE stock_items AS si
  SET 
  	quantity_on_hand = quantity_on_hand - sit.total_quantity
  FROM stock_item_totals AS sit
  WHERE si.id = sit.stock_item_id;
  
  --Customer Transactions
  INSERT INTO customer_transactions(
	  id,
	  transaction_date,
	  amount_excluding_tax,
	  tax_amount,
	  transaction_amount,
	  outstanding_balance,
	  finalization_date,
	  is_finalized,
	  last_edited_by,
	  last_edited_when,
	  customer_id,
	  payment_method_id,
	  transaction_type_id,
	  invoice_id,
	  order_id
  )
  SELECT 
  	nextval('sequence_generator'),
  	NOW(),
	(SELECT SUM(il.extended_price - il.tax_amount) FROM invoice_lines AS il WHERE il.invoice_id = itg.invoice_id),
	(SELECT SUM(il.tax_amount) FROM invoice_lines AS il WHERE il.invoice_id = itg.invoice_id),
	(SELECT SUM(il.extended_price) FROM invoice_lines AS il WHERE il.invoice_id = itg.invoice_id),
	(SELECT SUM(il.extended_price) FROM invoice_lines AS il WHERE il.invoice_id = itg.invoice_id),
	NULL,	
	NULL,
	_invoiced_by_person_id,
	NOW(),
  	i.bill_to_customer_id,
	i.payment_method_id,
	(SELECT id FROM transaction_types WHERE name = N'Customer Invoice'),
	itg.invoice_id,
	itg.order_id	
  FROM invoices_to_generate itg
  INNER JOIN invoices i
  ON itg.invoice_id = i.id;
  
  
  UPDATE orders
  SET status = 'PENDING'
  FROM invoices_to_generate itg 
  WHERE id = itg.order_id;
  --END TRANSACTION
  
 EXCEPTION
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
		RAISE NOTICE 'ERROR:%', l_context;
        _msg_error := SQLERRM;
		
  COMMIT;
--   CREATE TABLE rollback_table (id int);
--   INSERT INTO rollback_table VALUES (1);
--   ROLLBACK;
END $$;

