-- DROP FUNCTION cart_details(bigint);

CREATE OR REPLACE FUNCTION get_order_packages(_order_id bigint) RETURNS TEXT AS $$	
	DECLARE v_return text;
	
	BEGIN						
		SELECT INTO v_return 		
		(
			SELECT json_agg(
				json_build_object(
					'id', order_packages.id,
					'expectedDeliveryDate',order_packages.expected_delivery_date,
					'packageShippingFee',order_packages.package_shipping_fee,
					'packageShippingFeeDiscount',order_packages.package_shipping_fee_discount,
					'packagePrice',order_packages.package_price,
					'packageSubTotal',order_packages.package_price,
					'packageTaxAmount',order_packages.package_tax_amount,
					'packageVoucherDiscount',order_packages.package_voucher_discount,
					'packagePromotionDiscount',order_packages.package_promotion_discount,
					'customerReviewedOn',order_packages.customer_reviewed_on,
					'sellerRating',order_packages.seller_rating,
					'sellerReview',order_packages.seller_review,
					'deliveryRating',order_packages.delivery_rating,
					'deliveryReview',order_packages.delivery_review,
					'reviewAsAnonymous',order_packages.review_as_anonymous,
					'completedReview',order_packages.completed_review,
					'lastEditedBy',order_packages.last_edited_by,
					'orderLinesList',order_packages.order_line_list_json,
					'orderPackageDetails',order_packages.order_package_details
				)
			)
			FROM
			(
			SELECT 
				op.id,
				op.expected_delivery_date,
				op.package_shipping_fee,
				COALESCE(op.package_shipping_fee_discount,0) AS package_shipping_fee_discount,
				COALESCE(op.package_price,0) AS package_price,
				COALESCE(op.package_sub_total,0) AS package_sub_total,
				COALESCE(op.package_tax_amount,0) AS package_tax_amount,
				COALESCE(op.package_voucher_discount,0) AS package_voucher_discount,
				COALESCE(op.package_promotion_discount,0) AS package_promotion_discount,
				op.customer_reviewed_on,
				op.seller_rating,
				op.seller_review,
				op.delivery_rating,
				op.delivery_review,
				op.review_as_anonymous,
				op.completed_review,
				op.last_edited_by,
				(
					SELECT json_agg(
						json_build_object(
							'id',order_line_list.id,
							'description',order_line_list.description,
							'quantity',order_line_list.quantity,
							'taxRate',order_line_list.tax_rate,
							'unitPrice',order_line_list.unit_price,
							'unitPriceDiscount',order_line_list.unit_price_discount,
							'thumbnailUrl',order_line_list.thumbnail_url,
							'lineRating',order_line_list.line_rating,
							'lineReview',order_line_list.line_review,
							'customerReviewedOn',order_line_list.customer_reviewed_on,
							'supplierResponse',order_line_list.supplier_response,
							'supplierResponseOn',order_line_list.supplier_response_on,
							'likeCount',order_line_list.like_count,
							'lastEditedBy',order_line_list.last_edited_by,
							'supplierId',order_line_list.supplier_id,
							'supplierName',order_line_list.supplier_name,
							'reviewImageId',order_line_list.review_image_id,
							'reviewImageBlob',order_line_list.review_image_blob
						)
					) AS order_line_list_json
					FROM
					(
						SELECT 
							ol2.id,
							ol2.description,
							ol2.quantity,
							ol2.tax_rate,
							ol2.unit_price,
							ol2.unit_price_discount,
							ol2.thumbnail_url,
							ol2.line_rating,
							ol2.line_review,
							ol2.customer_reviewed_on,
							ol2.supplier_response,
							ol2.supplier_response_on,
							ol2.like_count,
							ol2.last_edited_by,
							sp.id AS supplier_id,
							sp.name AS supplier_name,
							ol2.review_image_id,
							(SELECT blob_id FROM photos WHERE id =  ol2.review_image_id)  AS review_image_blob			 	
						FROM order_lines ol2
						INNER JOIN suppliers sp
						ON ol2.supplier_id = sp.id
						WHERE ol2.order_package_id = op.id
					) order_line_list		
				),
				(
					SELECT to_json(
						json_build_object(
							'expectedMinArrivalDays',expected_arrival_days.expected_min_arrival_days,
							'expectedMaxArrivalDays',expected_arrival_days.expected_max_arrival_days
						)
					)
					FROM
					(SELECT 
							dm.expected_min_arrival_days,
							dm.expected_max_arrival_days
						FROM delivery_methods dm
						WHERE dm.id = op.delivery_method_id) expected_arrival_days	 			
				) AS order_package_details
			FROM orders o
			INNER JOIN order_packages op
			ON op.order_id = o.id
			INNER JOIN order_lines ol
			ON ol.order_package_id = op.id
			WHERE o.id = _order_id
			GROUP BY
				op.id,
				op.expected_delivery_date,
				op.package_shipping_fee,
				op.package_shipping_fee_discount,
				op.package_price,
				op.package_sub_total,
				op.package_tax_amount,
				op.package_voucher_discount,
				op.package_promotion_discount,
				op.customer_reviewed_on,
				op.seller_rating,
				op.seller_review,
				op.delivery_rating,
				op.review_as_anonymous,
				op.completed_review,
				op.last_edited_by
			) order_packages
		);

    RETURN v_return;
	END;
	
$$ LANGUAGE plpgsql;