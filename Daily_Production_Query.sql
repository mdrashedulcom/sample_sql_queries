DROP TABLE IF EXISTS custom_orders;

CREATE TABLE custom_orders AS

WITH
	torder AS
					(
					SELECT * FROM
					(
					SELECT
						id,
						lm_received_date,
-- 						shop_id,
-- 						delivery_area_id,
-- 						delivery_area_name,
-- 						lm_hub,
-- 						op_zone_name,
						parcel_count,
						record_created_at,
						ROW_NUMBER() OVER(PARTITION BY lm_received_date ORDER BY id DESC) AS sn
					FROM
						orders
					) t WHERE sn = 1 )
SELECT
	id,
	lm_received_date,
	record_created_at,
-- 	shop_id,
-- 	delivery_area_id,
-- 	delivery_area_name,
-- 	lm_hub,
-- 	op_zone_name,
	parcel_count
FROM
	torder;

DROP TABLE IF EXISTS custom_email;
CREATE TABLE custom_email AS

WITH wu AS (
						SELECT
							identifier,
							sr_reason,
							type,
							sub_type
						FROM
							(
							SELECT
								date,
								identifier,
								sr_reason,
								type,
								sub_type,
								ROW_NUMBER() OVER ( PARTITION BY identifier ORDER BY date ) AS sn 
							FROM
								wrapup 
							WHERE
								date >= '2022-09-18 00:00:00' 
								AND source = 'Email' 
								AND identifier REGEXP '^[0-9]+$' = 1
							) t1
						WHERE sn = 1
						)

SELECT
	e.ticket_id,
	e.mail_subject,
	e.mail_body_500,
	e.mail_status,
	e.priority,
	e.source,
	e.type,
	e.agent,
	CASE
		WHEN e.mail_group = 'RedX' THEN 'RedX'
		WHEN e.mail_group = 'Generic Email' THEN 'ShopUp'
		WHEN e.mail_group = 'Mokam Support' THEN 'Mokam'
		WHEN e.mail_group = 'Reseller Email' THEN 'ShopUp'
		WHEN e.mail_group = 'No Group' THEN 'ShopUp'
		WHEN e.mail_group = 'Teka Support' THEN 'ShopUp'
		ELSE 'ShopUp'
	END AS mail_group,	
	e.created_time,
	e.due_by_time,
	e.resolved_time,
	e.closed_time,
	e.last_update_time,
	e.initial_response_time,
	e.agent_interactions,
	e.customer_interactions,
	e.resolution_status,
	e.first_response_status,
	e.tags,		
	e.redx_sr_reason,
	e.redx_type,
	e.redx_subtype,
	e.mokam_sr_reason,
	e.mokam_type,
	e.mokam_subtype,
	e.shopup_sr_reason,
	e.shopup_type,
	e.shopup_subtype,
	
	CASE
		WHEN wu.sr_reason IS NOT NULL THEN wu.sr_reason
		WHEN e.mail_group = 'RedX' THEN redx_sr_reason
		WHEN e.mail_group = 'Mokam Support' THEN mokam_sr_reason
		ELSE e.shopup_sr_reason
	END	AS sr_reason,

	CASE
		WHEN wu.type IS NOT NULL THEN wu.type
		WHEN e.mail_group = 'RedX' THEN redx_type
		WHEN e.mail_group = 'Mokam Support' THEN mokam_type
		ELSE e.shopup_type
	END	AS sr_type,
	
	CASE
		WHEN wu.sub_type IS NOT NULL THEN wu.sub_type
		WHEN e.mail_group = 'RedX' THEN redx_subtype
		WHEN e.mail_group = 'Mokam Support' THEN mokam_subtype
		ELSE e.shopup_subtype
	END	AS sr_subtype,
	e.full_name,
	e.contact_id,
	e.remarks_500
FROM
	email e
	LEFT JOIN	wu ON e.ticket_id = wu.identifier
;

DROP TABLE IF EXISTS csat_response_temp;

CREATE TABLE csat_response_temp AS

SELECT
	'Customer' AS report_for,
	'No reason selected' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
	csat_score IS NOT NULL
	AND (reason_csat IS NULL OR reason_csat = '')
	
UNION

SELECT
	'Customer' AS report_for,
	'No one came for delivery' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_noone_came_for_delivery = 1

UNION

SELECT
	'Customer' AS report_for,
	'Product issue' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_product_issue = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'Agent refused door attempt' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_refused_door_attempt = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'Agent rude/unprofessional' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_rude_unprofessional = 1
	

UNION

SELECT
	'Customer' AS report_for,
	'Merchant seller communication' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_merchant_seller_communication = 1	
	
UNION

SELECT
	'Customer' AS report_for,
	'Agent forcefully cancelled' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_forcefully_cancelled = 1
	

UNION

SELECT
	'Customer' AS report_for,
	'Agent did not call' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_didnot_call = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'Agent rescheduled without approval' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_rescheduled_without_approval = 1
	
	
UNION

SELECT
	'Customer' AS report_for,
	'Customer did not reject delivery' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_customer_didnt_reject_delivery = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'Packaging issue' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_packaging_issue = 1

	
UNION

SELECT
	'Customer' AS report_for,
	'Agent asked tips' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_asked_tips = 1	

UNION

SELECT
	'Customer' AS report_for,
	'Agent did not give change after payment' AS csat_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	csat_score AS csat_response_type,
	additoinal_csat_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE	
	csat_agent_didnt_give_change_after_payment = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'No reason selected' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
	msat_score IS NOT NULL
	AND (reason_msat IS NULL OR reason_msat = '')

UNION

SELECT
	'Merchant' AS report_for,
	'Agent rude/unprofessional' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_agent_rude_unprofessional = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'Package improper during return' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_package_improper_during_return = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'Agent refused door attempt' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_agent_refused_door_attempt = 1

	
UNION

SELECT
	'Merchant' AS report_for,
	'KAM lack of support' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_kam_lack_of_support = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'Product damage after return' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_product_damage_after_return = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'Agent left parcels during pickup' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_agent_left_parcels_during_pickup = 1
	

	
UNION

SELECT
	'Merchant' AS report_for,
	'Product missing after return' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_product_missing_after_return = 1
	

UNION

SELECT
	'Merchant' AS report_for,
	'Lack of support from support team' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_lack_of_support_from_support_team = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'Payment related issue' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_payment_related_issue = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'No one came for pickup' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	msat_score,
	additoinal_msat_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name	
FROM
	merchant_nps
WHERE
	msat_noone_came_for_pickup = 1
;

DROP TABLE IF EXISTS nps_response_temp;

CREATE TABLE nps_response_temp AS

SELECT
	'Customer' AS report_for,
	'no_reason_selected' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
	nps_score > 0
	AND (reasons_of_nps_score IS NULL OR reasons_of_nps_score = '')

UNION

SELECT
	'Customer' AS report_for,
	'delivery_time_sla' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id AS parcel_or_shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub AS delivery_or_pickup_hub,
	delivery_zone_name AS delivery_or_pickup_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_delivery_time_sla = 1
	
UNION
	
SELECT
	'Customer' AS report_for,
	'delivery_agent_behaviour' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_delivery_agent_behaviour = 1
	
	
UNION

SELECT
	'Customer' AS report_for,
	'product_and_packaging_issue' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_product_and_packaging_issue = 1
		
UNION
	
SELECT
	'Customer' AS report_for,
	'payment_process_and_support' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_payment_process_and_support = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'doorstep_delivery_attempt' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_doorstep_delivery_attempt = 1
	
UNION

SELECT
	'Customer' AS report_for,
	'merchant_support_communication' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_merchant_support_communication = 1
	
UNION


SELECT
	'Customer' AS report_for,
	'app_site_performance_parcel_tracking' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_app_site_performance_parcel_tracking = 1
	
	
UNION


SELECT
	'Customer' AS report_for,
	'customer_support' AS nps_reason,
	campaign_id,
	response_created_at,
	type,
	parcel_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	delivery_hub,
	delivery_zone_name,
	agent_id,
	agent_name 
FROM
	customer_nps
WHERE
nps_customer_support = 1

UNION

SELECT
	'Merchant' AS report_for,
	'no_reason_selected' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
	nps_score >= 0
	AND (reasons_of_nps_score IS NULL OR reasons_of_nps_score = '')

UNION

SELECT
	'Merchant' AS report_for,
	'delivery_time_sla' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_delivery_time_sla = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'app_site_performance_parcel_tracking' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_app_site_performance_parcel_tracking = 1							
	
UNION

SELECT
	'Merchant' AS report_for,
	'customer_support_issue_resolution' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_customer_support_issue_resolution = 1

UNION

SELECT
	'Merchant' AS report_for,
	'customer_support' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_customer_support = 1

UNION

SELECT
	'Merchant' AS report_for,
	'issue_resolution' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_issue_resolution = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'payment_process' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_payment_process = 1


	
UNION

SELECT
	'Merchant' AS report_for,
	'kam_support_communication' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_kam_support_communication = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'product_handling' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_product_handling = 1
	
UNION

SELECT
	'Merchant' AS report_for,
	'pickup_process' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_pickup_process = 1
	
	
UNION

SELECT
	'Merchant' AS report_for,
	'return_process' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_return_process = 1
	


	
UNION

SELECT
	'Merchant' AS report_for,
	'pickup_return_agent_behaviour' AS nps_reason,
	campaign_answer_id,
	response_created_at,
	msat_type,
	shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE nps_score
	END AS nps_response_type,
	additoinal_nps_remarks_500,
	pickup_hub,
	pickup_zone,
	'N/A' AS pickup_agent_id,
	'N/A' AS pickup_agent_name 	
FROM
	merchant_nps
WHERE
nps_pickup_return_agent_behaviour = 1;


DROP TABLE IF EXISTS nps_temp;

CREATE TABLE nps_temp AS 

SELECT
	DATE(response_created_at) as report_date,
	DATE_FORMAT(response_created_at,'%Y-%V') as report_week,
	response_created_at,
	'Customer' AS report_for,
	campaign_id AS campaign_answer_id,
	type,
	parcel_id AS parcel_or_shop_id,
	nps_score,	
	CASE
		WHEN nps_score BETWEEN 1 AND 3 THEN 'detractor'
		WHEN nps_score = 4 THEN 'passive'
		WHEN nps_score = 5 THEN 'promoter'
		ELSE NULL
	END AS nps_point,
	csat_score,
	reasons_of_nps_score,
	additoinal_nps_remarks_500
FROM
	customer_nps
	
UNION ALL

SELECT
	DATE(response_created_at) as report_date,
	DATE_FORMAT(response_created_at,'%Y-%V') as report_week,
	response_created_at,
	'Merchant' AS report_for,
	campaign_answer_id,
	msat_type as type,
	shop_id AS parcel_or_shop_id,
	nps_score,
	CASE
		WHEN nps_score BETWEEN 0 AND 6 THEN 'detractor'
		WHEN nps_score BETWEEN 7 AND 8 THEN 'passive'
		WHEN nps_score BETWEEN 9 AND 10 THEN 'promoter'
		ELSE NULL
	END AS nps_point,
	msat_score,
	reasons_of_nps_score,
	additoinal_nps_remarks_500
FROM
	merchant_nps
;


DROP TABLE IF EXISTS agent_summary;

CREATE TABLE agent_summary AS 

SELECT
	agent_id,
	agent_name,
	CASE
		WHEN COUNT(DISTINCT DATE(call_started_at)) >= 30 THEN 'Old Agent'
		ELSE 'New Agent'
	END AS agent_type,
	MIN( call_started_at ) AS first_working_date,
	MAX( call_started_at ) AS last_working_date,
	COUNT(DISTINCT DATE(call_started_at)) AS active_days,
	COUNT(DISTINCT call_id) AS call_received,
	DATEDIFF(CURRENT_TIMESTAMP - INTERVAL 1 DAY, MAX( call_started_at )) AS intactive_days,
	CASE
		WHEN DATEDIFF(CURRENT_TIMESTAMP - INTERVAL 1 DAY, MAX( call_started_at )) > 7 THEN 'Iactive > 7 Days'
		WHEN DATEDIFF(CURRENT_TIMESTAMP - INTERVAL 1 DAY, MAX( call_started_at )) > 2 THEN 'Iactive > 2 Days'
		ELSE 'Active Agent'
	END AS Agent_Status
FROM
	inbound_full
WHERE
	agent_id LIKE '10%'
	OR agent_id LIKE '20%'
	AND business_name != 'Test'
GROUP BY
	agent_id,
	agent_name
ORDER BY
	last_working_date;
	
	
DROP TABLE IF EXISTS custom_issue;

CREATE TABLE custom_issue AS 

SELECT DISTINCT
-- 	CONCAT('WK-',WEEK(issue_created_at,0)) AS report_week,
	DATE_FORMAT(Issue_created_at,'%XW%V') AS report_week,
	DATE(issue_created_at) AS created_on,
	zone.op_zone_name AS zone_name,
	zone.coverage AS area,
	fc.final_group,
	issue_tbl.*
FROM
	(
	SELECT
		i.issue_created_at,
		i.tracking_id,
		i.issue_id,
		i.parcel_id,
		i.ir_approval_status,
		i.escalated_by,
		CASE
			WHEN i.escalation_tag IS NULL THEN 'operation'
			ELSE i.escalation_tag
		END AS issue_raised_by,
		i.investigation_started_at,
		i.investigation_started_by,
		i.resolution_date,
		IF(i.resolution_date > '2020-01-01 00:00:00', 'Solved', 'Pending') AS custom_issue_status,
		CASE
			WHEN TIMESTAMPDIFF(HOUR, i.issue_created_at, (
																									CASE
																										WHEN i.resolution_date IS NULL THEN CURRENT_TIMESTAMP
																										ELSE i.resolution_date
																									END)) >= 48 THEN 'SLA Breached'
			ELSE 'Within SLA'
		END AS custom_sla_status,
		i.resolved_by,
		i.approved_by,
		i.approved_at,
		i.issue_status,
		i.finance_satus,
		i.compensation_approval_panel_status,
		i.escalation_dashboard_status,
		i.bulk_issue_status,
		i.parcel_status,
		i.bulk_status,
		i.bulk_trasfer_id,
		i.bulk_id,
		CASE
			WHEN i.current_or_responsible_hub IS NULL THEN i.pickub_hub
			WHEN i.issue_group IN (
														'Delay in Pickup',
														'Pickup is showing as Pending',
														'Pickup man did not arrive'
														) THEN i.pickub_hub
			ELSE i.current_or_responsible_hub
		END AS responsible_hub,
		i.pickub_hub,
		i.source_hub,
		i.destination_hub,
		i.issue_raised_hub_info,
		i.value,
		i.cash,
		i.shop_id,
		i.shop_name,
		i.is_problematic,
		i.compensation_amount,
		i.ir_compensation_amount,
		i.last_status_updated_at,
		i.payment_type,
		i.is_adjustment_settled,
		i.logistic_invoice_id,
		i.is_escalated_by_merchant,
		i.issue_group,
		i.issue_type,
		i.payment_type_2,
		i.finance_got_this_issue_at,
		i.current_finance_state,
		i.delivery_hub,
		i.combined_invoice_id,
		i.is_paid,
		i.merchant_type,
		i.team_name
	FROM
		issues i
	WHERE
		i.issue_group NOT IN ('Cancel the delivery', 'Parcel Cancellation by customer', 'Delivery/Return Charge adjustment')
	) issue_tbl

	LEFT JOIN

	(
	SELECT
		*
	FROM
		zone_info
	) zone

	ON issue_tbl.responsible_hub = zone.hub_name
	
	LEFT JOIN
	
	(
	SELECT issue_group, issue_type, final_group  FROM category_wise_issue_escalations
	) fc
	
	ON issue_tbl.issue_group = fc.issue_group AND issue_tbl.issue_type = fc.issue_type
;


SELECT 'email' AS table_name, MIN(created_time) AS data_started_from, MAX(created_time) as last_update_at, COUNT(*) AS numbers_of_row FROM email

UNION

SELECT 'custom_email' AS table_name, MIN(created_time) AS data_started_from, MAX(created_time) as last_update_at, COUNT(*) AS numbers_of_row FROM custom_email

UNION

SELECT 'agent_report' AS table_name, MIN(first_login_time) AS data_started_from, MAX(first_login_time) as last_update_at, COUNT(*) AS numbers_of_row FROM agent_report

UNION

SELECT 'agent_summary' AS table_name, MIN(last_working_date) AS data_started_from, MAX(last_working_date) as last_update_at, COUNT(*) AS numbers_of_row FROM agent_summary

UNION

SELECT 'bot' AS table_name, MIN(started_at) AS data_started_from, MAX(started_at) as last_update_at, COUNT(*) AS numbers_of_row FROM bot

UNION

SELECT 'issues' AS table_name, MIN(issue_created_at) AS data_started_from, MAX(issue_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM issues

UNION

SELECT 'custom_issue' AS table_name, MIN(issue_created_at) AS data_started_from, MAX(issue_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM custom_issue

UNION

SELECT 'customer_nps' AS table_name, MIN(response_created_at) AS data_started_from, MAX(response_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM customer_nps

UNION

SELECT 'merchant_nps' AS table_name, MIN(response_created_at) AS data_started_from, MAX(response_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM merchant_nps

UNION

SELECT 'nps_temp' AS table_name, MIN(response_created_at) AS data_started_from, MAX(response_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM nps_temp

UNION

SELECT 'nps_response_temp' AS table_name, MIN(response_created_at) AS data_started_from, MAX(response_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM nps_response_temp

UNION

SELECT 'csat_response_temp' AS table_name, MIN(response_created_at) AS data_started_from, MAX(response_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM csat_response_temp

UNION

SELECT 'page_and_group_followup' AS table_name, MIN(dealing_at) AS data_started_from, MAX(dealing_at) as last_update_at, COUNT(*) AS numbers_of_row FROM page_and_group_followup

UNION

SELECT 'inbound_full' AS table_name, MIN(call_started_at) AS data_started_from, MAX(call_started_at) as last_update_at, 
COUNT(*) AS numbers_of_row FROM inbound_full

UNION

SELECT 'kam_issue' AS table_name, MIN(time_stamp) AS data_started_from, MAX(time_stamp) as last_update_at, COUNT(*) AS numbers_of_row FROM kam_issue

UNION

SELECT 'sm_activity' AS table_name, MIN(arrived_at) AS data_started_from, MAX(arrived_at) as last_update_at, COUNT(*) AS numbers_of_row FROM sm_activity

UNION

SELECT 'custom_orders' AS table_name, MIN(record_created_at) AS data_started_from, MAX(record_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM custom_orders

UNION

SELECT 'orders' AS table_name, MIN(record_created_at) AS data_started_from, MAX(record_created_at) as last_update_at, COUNT(*) AS numbers_of_row FROM orders

ORDER BY 3
;