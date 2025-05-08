-- 1. Data interity issue
-- Ensures Data Integrity: Only the most relevant and accurate pricing data is used.
-- Prevents Duplicate Data from Impacting Queries: Reduces redundancy in queries and reports.
EXPLAIN ANALYZE
WITH product_pricing_cte AS (
	SELECT
		b.brand_name,
		m.model_name,
		c.color,
		sm.strap_material,
		d.display,
		dt.device_type,
		p.selling_price,
		p.original_price,
		CASE
			WHEN p.is_effective = true THEN 1
			ELSE 0
		END AS is_price_effective,
		CONCAT(b.brand_id, m.model_id, c.color_id, sm.strap_id, d.display_id, dt.device_type_id) AS identifier
	FROM tracker_data.fact_pricing p
		INNER JOIN tracker_data.dim_models m ON p.model_id = m.model_id
		INNER JOIN tracker_data.dim_colors c ON p.color_id = c.color_id
		INNER JOIN tracker_data.dim_brands b ON p.brand_id = b.brand_id
		INNER JOIN tracker_data.dim_strap_materials sm ON p.strap_id = sm.strap_id
		INNER JOIN tracker_data.dim_displays d ON p.display_id = d.display_id
		INNER JOIN tracker_data.dim_device_types dt ON p.device_type_id = dt.device_type_id
	ORDER BY brand_name, model_name, color
)
SELECT
	brand_name,
	model_name,
	color,
	strap_material,
	display,
	device_type,
	selling_price,
	original_price,
	is_price_effective
FROM (
	SELECT
		*,
		LEAD(identifier, 1) OVER (ORDER BY identifier) AS next_tracker,
		LAG(identifier, 1) OVER (ORDER BY identifier) AS prev_tracker
	FROM product_pricing_cte
) AS track_record
WHERE (identifier = next_tracker AND identifier <> prev_tracker)
	OR (identifier = next_tracker AND identifier = prev_tracker)
	OR (identifier <> next_tracker AND identifier = prev_tracker)
ORDER BY identifier;	

-- 2. Resolving Data Inconsistency and Redundancy Issues
-- data inconsistency
SELECT DISTINCT(strap_material) FROM tracker_data.fitness_trackers_raw;

-- "Leather"
-- "Leather "
-- "leather"

EXPLAIN ANALYZE
WITH brand_straps_cte AS (
	SELECT
		b.brand_name,
		bs.strap_id
	FROM tracker_data.dim_brands b
		INNER JOIN tracker_data.brand_straps bs ON b.brand_id = bs.brand_id	
)
SELECT
	bsc.brand_name,
	sm.strap_material
FROM brand_straps_cte bsc
	INNER JOIN tracker_data.dim_strap_materials sm ON bsc.strap_id = sm.strap_id
WHERE strap_material ILIKE '_eather';

-- data redundany
SELECT * FROM tracker_data.fitness_trackers_raw;

-- -- color (e.g. 'black' vs 'Black', 'multicolor' vs 'Multicolor')
-- SELECT DISTINCT(color)
-- FROM tracker_data.fitness_trackers_raw
-- WHERE color ILIKE '_lack'
-- OR color ILIKE '_ulticolor';

-- SELECT * FROM tracker_data.dim_colors ORDER BY color;

--EXPLAIN ANALYZE
WITH color_cte AS (
	SELECT
		mcd.model_id,
		c.color_id,
		c.color
	FROM tracker_data.dim_colors c
		INNER JOIN tracker_data.model_color_details mcd
			ON c.color_id = mcd.color_id
)
SELECT
	m.model_name,
	C.color_id,
	c.color
FROM color_cte c
	INNER JOIN tracker_data.dim_models m
		ON c.model_id = m.model_id
WHERE c.color_id = 'C3';

SELECT * FROM tracker_data.dim_colors ORDER BY color;

-- 3. Data Integration issue eliminated
-- use patient_device_id combining with medical-related tables and device-related tables
EXPLAIN ANALYZE
WITH patient_device_cte AS (
	-- get patient device information
	SELECT
		p.patient_id,
		hm.patient_device_id,
		p.first_name,
		p.last_name,
		wd.brand_id,
		wd.device_type_id,
		wd.model_id
	FROM health_data.dim_patients p
		INNER JOIN tracker_data.fact_health_metrics hm
			ON p.patient_id = hm.patient_id
		INNER JOIN tracker_data.wearable_devices wd
			ON hm.patient_device_id = wd.patient_device_id
),
brand_name_cte AS (
	-- get the brand name of the patient's device
	SELECT
		pd.*,
		b.brand_name
	FROM patient_device_cte pd
		INNER JOIN tracker_data.dim_brands b
			ON pd.brand_id = b.brand_id	
),
device_name_cte AS (
	-- get the device name of the patient's device
	SELECT
		pd.*,
		dt.device_type
	FROM patient_device_cte pd
		INNER JOIN tracker_data.dim_device_types dt
			ON pd.device_type_id = dt.device_type_id	
),
model_name_cte AS (
	-- get the model name of the patient's device
	SELECT
		pd.*,
		m.model_name
	FROM patient_device_cte pd
		INNER JOIN tracker_data.dim_models m
			ON pd.model_id = m.model_id	
)
SELECT bn.patient_id,
	bn.patient_device_id,
	bn.first_name,
	bn.last_name,
	bn.brand_name,
	dn.device_type,
	mn.model_name
FROM brand_name_cte bn
	INNER JOIN device_name_cte dn
		ON bn.patient_id = dn.patient_id AND bn.patient_device_id = dn.patient_device_id
	INNER JOIN model_name_cte mn
		ON dn.patient_id = mn.patient_id AND dn.patient_device_id = mn.patient_device_id
WHERE bn.first_name = 'Kristen'
AND bn.last_name = 'Weaver';

