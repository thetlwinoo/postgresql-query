CREATE OR REPLACE FUNCTION get_product_details(_product_id bigint) RETURNS TEXT AS $$	
	DECLARE v_return text;
	
	BEGIN
		SELECT INTO v_return
		(
			SELECT to_json(json_build_object(
				'id',product_objects.product_id,
				'name',product_objects.product_name,
				'sellCount',product_objects.sell_count,
				'productCategoryName',product_objects.product_category,
				'productBrandName',product_objects.product_brand,
				'videoUrl',product_objects.video_url,
				'highlights',product_objects.highlights,
				'longDescription',product_objects.long_description,
				'shortDescription',product_objects.short_description,
				'whatInTheBox',product_objects.what_in_the_box,
				'careInstructions',product_objects.care_instructions,
				'productType',product_objects.product_type,
				'modelName',product_objects.model_name,
				'modelNumber',product_objects.model_number,
				'fabricType',product_objects.fabric_type,
				'specialFeatures',product_objects.special_features,
				'productComplianceCertificate',product_objects.product_compliance_certificate,
				'genuineAndLegal',product_objects.genuine_and_legal,
				'countryOfOrigin',product_objects.country_of_origin,
				'usageAndSideEffects',product_objects.usage_and_side_effects,
				'safetyWarnning',product_objects.safety_warnning,
				'warrantyPeriod',product_objects.warranty_period,
				'warrantyPolicy',product_objects.warranty_policy,
				'dangerousGoods',product_objects.dangerous_goods,
				'warrantyTypeId',product_objects.warranty_type_id,
				'warrantyTypeName',product_objects.warranty_type_name,
				'ratings',product_objects.rating_json,
				'stockItemList',product_objects.stock_items_json,
				'reviewList',product_objects.reviews_json,
				'relatedList',product_objects.related_json
			))
			FROM
			(SELECT 
				pd.id AS product_id,
				pd.name AS product_name,
				pd.sell_count AS sell_count,
				pc.name AS product_category,
				pb.name AS product_brand,
				pm.video_url,
				pm.highlights,
				pm.long_description,
				pm.short_description,
				pm.what_in_the_box,
				pm.care_instructions,
				pm.product_type,
				pm.model_name,
				pm.model_number,
				pm.fabric_type,
				pm.special_features,
				pm.product_compliance_certificate,
				pm.genuine_and_legal,
				pm.country_of_origin,
				pm.usage_and_side_effects,
				pm.safety_warnning,
				pm.warranty_period,
				pm.warranty_policy,
				pm.dangerous_goods,
				pm.warranty_type_id,
				wt.name AS warranty_type_name,
				(
					SELECT to_json(
						json_build_object(
							'fiveStars',ratings.five_stars,
							'fiveStarsPercentage',(ratings.five_stars * 100) / (CASE ratings.total_stars WHEN 0 THEN 1 ELSE ratings.total_stars END),
							'fourStars',ratings.four_stars,
							'fourStarsPercentage',(ratings.four_stars * 100) / (CASE ratings.total_stars WHEN 0 THEN 1 ELSE ratings.total_stars END),
							'threeStars',ratings.three_stars,
							'threeStarsPercentage',(ratings.three_stars * 100) / (CASE ratings.total_stars WHEN 0 THEN 1 ELSE ratings.total_stars END),
							'twoStars',ratings.two_stars,
							'twoStarsPercentage',(ratings.two_stars * 100) / (CASE ratings.total_stars WHEN 0 THEN 1 ELSE ratings.total_stars END),
							'oneStars',ratings.one_stars,
							'oneStarsPercentage',(ratings.one_stars * 100) / (CASE ratings.total_stars WHEN 0 THEN 1 ELSE ratings.total_stars END),
							'overallRating',ratings.total_stars / (CASE ratings.total_rating WHEN 0 THEN 1 ELSE ratings.total_rating END),
							'totalRating',ratings.total_rating
						)
					)
					FROM
					(SELECT 
						COUNT(*) FILTER (WHERE ol2.line_rating = 5) five_stars,	
						COUNT(*) FILTER (WHERE ol2.line_rating = 4) four_stars,
						COUNT(*) FILTER (WHERE ol2.line_rating = 3) three_stars,
						COUNT(*) FILTER (WHERE ol2.line_rating = 2) two_stars,
						COUNT(*) FILTER (WHERE ol2.line_rating = 1) one_stars,
					 	((5 * COUNT(*) FILTER (WHERE ol2.line_rating = 5)) + (4 * COUNT(*) FILTER (WHERE ol2.line_rating = 4)) + (3 * COUNT(*) FILTER (WHERE ol2.line_rating = 3)) + (2 * COUNT(*) FILTER (WHERE ol2.line_rating = 2)) +(1 * COUNT(*) FILTER (WHERE ol2.line_rating = 1))) AS total_stars,
						(COUNT(*) FILTER (WHERE ol2.line_rating = 5) + COUNT(*) FILTER (WHERE ol2.line_rating = 4) + COUNT(*) FILTER (WHERE ol2.line_rating = 3) + COUNT(*) FILTER (WHERE ol2.line_rating = 2) +COUNT(*) FILTER (WHERE ol2.line_rating = 1)) AS total_rating
					FROM order_lines ol2
					INNER JOIN stock_items st2
					ON ol2.stock_item_id = st2.id
					WHERE st2.product_id = pd.id) as ratings) AS rating_json,
				(
					SELECT json_agg(json_build_object(
						'id',stock_items.stock_item_id,
						'name',stock_items.stock_item_name,
						'thumbnailUrl',stock_items.thumbnail_url,
						'unitPrice',stock_items.unit_price,
						'recommendedRetailPrice',stock_items.recommended_retail_price,
						'quantityOnHand',stock_items.quantity_on_hand,
						'productAttributeName',stock_items.product_attribute,
						'productOptionName',stock_items.product_option,
						'photoList',stock_items.photos
					))
					FROM
					(SELECT 
						st3.id AS stock_item_id,
						st3.name AS stock_item_name,
					 	st3.thumbnail_url,
						st3.unit_price,
						st3.recommended_retail_price,
						st3.quantity_on_hand,
					 	pa3.value AS product_attribute,
					 	po3.value AS product_option,
						(
							SELECT json_agg(json_build_object(
								'blobId',photos_json.blob_id,
								'photoName',photos_json.photo_name
							))
							FROM
							(
								SELECT 
								 pt.blob_id,
								 st3.name AS photo_name
								FROM photos pt
								WHERE pt.stock_item_id = st3.id
							) photos_json
						) AS photos
					FROM stock_items st3
					INNER JOIN product_attribute pa3
					ON st3.product_attribute_id = pa3.id
					INNER JOIN product_option po3
					ON st3.product_option_id = po3.id
					WHERE st3.product_id = pd.id) stock_items
				) AS stock_items_json,
				(
					SELECT json_agg(json_build_object(
						'customerId',reviews.customer_id,
						'customerName',reviews.customer_name,
						'productAttribute',reviews.product_attribute,
						'productOption',reviews.product_option,
						'lineRating',reviews.line_rating,
						'lineReview',reviews.line_review,
						'customerReviewedOn',reviews.customer_reviewed_on,
						'supplierResponse',reviews.supplier_response,
						'supplierResponseOn',reviews.supplier_response_on,
						'supplierId',reviews.supplier_id,
						'supplierName',reviews.supplier_name,
						'likeCount',reviews.like_count,
						'reviewImageId',reviews.review_image_id,
						'reviewImageBlob',reviews.review_image_blob
					))
					FROM 
					(SELECT 
						od4.customer_id,
						ct4.name AS customer_name,
						CONCAT(pas4.name,' - ',pa4.value) AS product_attribute,
						CONCAT(pos4.value,' - ',po4.value) AS product_option,
						ol4.line_rating,
						ol4.line_review,
						ol4.customer_reviewed_on,
						ol4.supplier_response,
						ol4.supplier_response_on,
						ol4.supplier_id,
						sp4.name AS supplier_name,
						ol4.like_count,
						ol4.review_image_id,
						ph4.blob_id AS review_image_blob
					FROM order_lines ol4
					INNER JOIN stock_items st4
					ON ol4.stock_item_id = st4.id
					INNER JOIN suppliers sp4
					ON ol4.supplier_id = sp4.id
					INNER JOIN photos ph4
					ON ol4.review_image_id = ph4.id
					INNER JOIN product_attribute pa4
					ON st4.product_attribute_id = pa4.id
					INNER JOIN product_attribute_set pas4
					ON pa4.product_attribute_set_id = pas4.id
					INNER JOIN product_option po4
					ON st4.product_option_id = po4.id
					INNER JOIN product_option_set pos4
					ON po4.product_option_set_id = pos4.id
					INNER JOIN order_packages op4
					ON ol4.order_package_id = op4.id
					INNER JOIN orders od4
					ON op4.order_id = od4.id
					INNER JOIN customers ct4
					ON od4.customer_id = ct4.id
					WHERE st4.product_id =  pd.id
					AND ol4.line_rating is not null) reviews
				) AS reviews_json,
			 	(
					SELECT json_agg(json_build_object(
						'name',related.name,
						'handle',related.handle,
						'productDetails',related.product_details,
						'totalStars',related.total_stars,
						'discountedPercentage',related.discounted_percentage,
						'preferredInd',related.preferred_ind,
						'availableDeliveryInd',related.available_delivery_ind,
						'productCategoryName',related.product_category,
						'productBrandName',related.product_brand
					))
					FROM
					(SELECT 
						pd5.name,
						pd5.handle,
						pd5.product_details::json AS product_details,
						pd5.total_stars,
						pd5.discounted_percentage,
						pd5.preferred_ind,
						pd5.available_delivery_ind,
						pc5.name AS product_category,
						pb5.name AS product_brand
					FROM products pd5
					INNER JOIN product_category pc5
					ON pd5.product_category_id = pc5.id
					INNER JOIN product_brand pb5
					ON pd5.product_brand_id = pb5.id
					WHERE pd5.product_category_id = 10747
					LIMIT 12
					) related
				) AS related_json
			FROM products pd
			INNER JOIN product_category pc
			ON pd.product_category_id = pc.id
			INNER JOIN product_brand pb
			ON pd.product_brand_id = pb.id
			INNER JOIN product_document pm
			ON pd.product_document_id = pm.id
			INNER JOIN warranty_types wt
			ON pm.warranty_type_id = wt.id
			WHERE pd.id = _product_id) AS product_objects
		);
		
	RETURN v_return;	
	END;
	
$$ LANGUAGE plpgsql;