-- 1. Data duplication, data Inconsistency, and data quality issues
CREATE MATERIALIZED VIEW tracker_data.vw_product_pricing
AS
	SELECT
		*,
		LEAD(identifier, 1) OVER (ORDER BY identifier) AS next_tracker,
		LAG(identifier, 1) OVER (ORDER BY identifier) AS prev_tracker
	FROM (
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
	) AS track_record;

CREATE INDEX idx_product_pricing
	ON tracker_data.vw_product_pricing(identifier, next_tracker, prev_tracker)
	WHERE (identifier = next_tracker AND identifier <> prev_tracker)
	OR (identifier = next_tracker AND identifier = prev_tracker)
	OR (identifier <> next_tracker AND identifier = prev_tracker);

EXPLAIN ANALYZE
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
FROM tracker_data.vw_product_pricing
WHERE (identifier = next_tracker AND identifier <> prev_tracker)
	OR (identifier = next_tracker AND identifier = prev_tracker)
	OR (identifier <> next_tracker AND identifier = prev_tracker)
ORDER BY identifier;	

-- 2. Resolving Data Inconsistency and Standardization Issues
-- strap_material (e.g. 'leather' vs 'Leather')
CREATE MATERIALIZED VIEW tracker_data.vw_brand_straps
AS
	SELECT
	    b.brand_name,
	    sm.strap_material
	FROM tracker_data.dim_brands b
		INNER JOIN tracker_data.brand_straps bs ON b.brand_id = bs.brand_id
		INNER JOIN tracker_data.dim_strap_materials sm ON bs.strap_id = sm.strap_id;

CREATE INDEX idx_brand_straps
	ON tracker_data.vw_brand_straps(strap_material);

-- row_number | strap_material      |  -->    Index applied
--     1      |    Leather          |            Leather    --> row number 1, 4
--     2      |    Plastic          |            Plastic    --> row number 2, 3
--     3      |    Plastic          |            Fabric     --> row number 5
--     4      |    Leather          |
--     5      |    Fabric           |


EXPLAIN ANALYZE
SELECT *
FROM tracker_data.vw_brand_straps
WHERE strap_material ILIKE '_eather';

-- color (e.g. 'black' vs 'Black', 'multicolor' vs 'Multicolor')
CREATE MATERIALIZED VIEW tracker_data.vw_model_colors
AS
	SELECT
		m.model_name,
		c.color
	FROM tracker_data.dim_colors c
		INNER JOIN tracker_data.model_color_details mcd ON c.color_id = mcd.color_id
		INNER JOIN tracker_data.dim_models m ON mcd.model_id = m.model_id;

CREATE INDEX idx_model_colors
	ON tracker_data.vw_model_colors(model_name, color);

EXPLAIN ANALYZE
SELECT *
FROM tracker_data.vw_model_colors
WHERE (color ILIKE '_lack')
	OR (color ILIKE '_ulticolor')
ORDER BY color;

-- 3. Data Integrity & Accuracy
CREATE MATERIALIZED VIEW tracker_data.vw_patient_devices
AS
	SELECT 
	    p.patient_id,
	    hm.patient_device_id,
	    p.first_name,
	    p.last_name,
	    b.brand_name,
	    dt.device_type,
	    m.model_name
	FROM health_data.dim_patients p
	INNER JOIN tracker_data.fact_health_metrics hm
	    ON p.patient_id = hm.patient_id
	INNER JOIN tracker_data.wearable_devices wd
	    ON hm.patient_device_id = wd.patient_device_id
	INNER JOIN tracker_data.dim_brands b
	    ON wd.brand_id = b.brand_id
	INNER JOIN tracker_data.dim_device_types dt
	    ON wd.device_type_id = dt.device_type_id
	INNER JOIN tracker_data.dim_models m
	    ON wd.model_id = m.model_id;

CREATE INDEX idx_patient_devices
	ON tracker_data.vw_patient_devices(first_name, last_name);

EXPLAIN ANALYZE
SELECT *
FROM tracker_data.vw_patient_devices
WHERE first_name = 'Kristen'
AND last_name = 'Weaver';


