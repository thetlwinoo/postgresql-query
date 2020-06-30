CREATE OR REPLACE FUNCTION calculate_customer_price(_customer_id integer,_stock_item_id integer,_pricing_date date) RETURNS numeric(21,2) AS $$

	DECLARE v_calculated_price numeric(21,2);
    DECLARE v_unit_price numeric(21,2);
    DECLARE v_lowest_unit_price numeric(21,2);
    DECLARE v_highest_discount_amount numeric(21,2);
    DECLARE v_highest_discount_percentage numeric(21,2);
    DECLARE v_discounted_unit_price numeric(21,2);
	
	BEGIN

		SELECT INTO v_unit_price si.unit_price
		FROM stock_items AS si
		WHERE si.id = _stock_item_id; 

		SELECT INTO v_calculated_price v_unit_price;		
		
		SELECT INTO v_lowest_unit_price (
			SELECT MIN(sd.unit_price)
			FROM special_deals AS sd
			WHERE ((sd.stock_item_id = _stock_item_id) OR (sd.stock_item_id IS NULL))
			AND ((sd.customer_id = _customer_id) OR (sd.customer_id IS NULL))						
			AND sd.unit_price IS NOT NULL
			AND _pricing_date BETWEEN sd.start_date AND sd.end_date);	
		
		IF v_lowest_unit_price IS NOT NULL AND v_lowest_unit_price < v_unit_price THEN
			PERFORM v_calculated_price = v_lowest_unit_price;
		END IF;
		
		SELECT INTO v_highest_discount_amount (
			SELECT MAX(sd.discount_amount)
			FROM special_deals AS sd
			WHERE ((sd.stock_item_id = _stock_item_id) OR (sd.stock_item_id IS NULL))
			AND ((sd.customer_id = _customer_id) OR (sd.customer_id IS NULL))			
			AND sd.discount_amount IS NOT NULL
			AND _pricing_date BETWEEN sd.start_date AND sd.end_date);
			
		IF v_highest_discount_amount IS NOT NULL AND (v_unit_price - v_highest_discount_amount) < v_calculated_price THEN		
			SELECT INTO v_calculated_price (v_unit_price - v_highest_discount_amount);
		END IF;
		
		SELECT INTO v_highest_discount_percentage (
			SELECT MAX(sd.discount_percentage)
			FROM special_deals AS sd
			WHERE ((sd.stock_item_id = _stock_item_id) OR (sd.stock_item_id IS NULL))
			AND ((sd.customer_id = _customer_id) OR (sd.customer_id IS NULL))			
			AND sd.discount_amount IS NOT NULL
			AND _pricing_date BETWEEN sd.start_date AND sd.end_date);

    IF v_highest_discount_percentage IS NOT NULL THEN
	
        SELECT INTO v_discounted_unit_price ROUND(v_unit_price * v_highest_discount_percentage / 100.0, 2);
		
        IF v_discounted_unit_price < v_calculated_price THEN
			SELECT INTO v_calculated_price v_discounted_unit_price;
		END IF;
		
    END IF;	

	RAISE NOTICE 'Unit Price: %', v_unit_price; 
	RAISE NOTICE 'Lowest Unit Price: %', v_lowest_unit_price;
	RAISE NOTICE 'Highest Discount Amount: %', v_highest_discount_amount;
	RAISE NOTICE 'Highest Discount Percentage: %', v_highest_discount_percentage;
	RAISE NOTICE 'Calculated Price: %', v_calculated_price;
	
    RETURN v_calculated_price;
	
	END;
	
$$ LANGUAGE plpgsql;