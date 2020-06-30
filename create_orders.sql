CREATE OR REPLACE PROCEDURE create_orders(
	_cart_id IN BIGINT,
	_msg_error INOUT VARCHAR
) 
LANGUAGE plpgsql 
AS $$
DECLARE
  l_context text;
  next_order_id BIGINT;
  purchase_order_number text;
BEGIN	

	SELECT next_order_id = nextval('sequence_generator');
	SELECT purchase_order_number = uuid_generate_v4();
	
  --Orders
  INSERT INTO orders
  SELECT 
  	next_order_id,
	NOW(),
	sc.total_price - sc.total_cargo_price,
	sc.total_price * 0.07,
  	sc.total_cargo_price,
	sc.total_price,
	CURRENT_DATE + INTERVAL '3 day',
	'PENDING',
	purchase_order_number,
	NULL,
	NULL,
	NULL,
	NULL,
	'NEW_ORDER',
	NULL, --order details
	false,
	'SYSTEM',
	NOW(),
	sc.customer_id,
	c.delivery_address_id,
	c.delivery_address_id,
	(SELECT id FROM delivery_methods WHERE name = N'Delivery Van'),
	NULL,
	NULL,	
	NULL,
	sc.special_deals_id
  FROM shopping_carts sc
  INNER JOIN customers c
  ON sc.customer_id = c.id
  WHERE sc.id = _cart_id;
  
  INSERT INTO order_packages    
  SELECT 
  	nextval('sequence_generator'),
  	CURRENT_DATE + INTERVAL '3 day',
	NULL,
	NULL,
	NULL,
	purchase_order_number,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	'SYSTEM',
	NOW(),
  	si.supplier_id,
	next_order_id
  FROM shopping_cart_items sci
  INNER JOIN stock_items si
  ON sci.stock_item_id = si.id
  WHERE sci.cart_id = _cart_id;
  
  --END TRANSACTION  
 EXCEPTION
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
		RAISE NOTICE 'ERROR:%', l_context;
        _msg_error := SQLERRM;
		
  COMMIT;
END $$;

