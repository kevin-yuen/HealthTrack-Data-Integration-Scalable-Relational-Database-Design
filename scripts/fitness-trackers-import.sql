CREATE SCHEMA IF NOT EXISTS tracker_data;

DROP TABLE IF EXISTS tracker_data.fitness_trackers_raw;

-- step 1: create tracker_data.fitness_trackers_raw for storing raw data
CREATE TABLE IF NOT EXISTS tracker_data.fitness_trackers_raw (
	brand_name VARCHAR(200) NOT NULL,
	device_type VARCHAR(250) NOT NULL,
	model_name VARCHAR(500) NOT NULL,
	color VARCHAR(300) NOT NULL,
	selling_price TEXT NOT NULL,
	original_price TEXT NOT NULL,
	display VARCHAR(500) NOT NULL,
	rating FLOAT,
	strap_material VARCHAR(500) NOT NULL,
	average_battery_life INT,
	reviews TEXT
);

-- step 2: import raw data from csv file to tracker_data.fitness_trackers_raw table (via 'Import/Export Data' function)

-- step 3: verify that data has sucessfully ingested
SELECT * FROM tracker_data.fitness_trackers_raw;

-- step 4: transformation - clean up data
-- ### step 4.1: cast to appropriate data type for each column
ALTER TABLE tracker_data.fitness_trackers_raw
ALTER COLUMN selling_price TYPE FLOAT USING REPLACE(selling_price, ',', '')::float,
ALTER COLUMN original_price TYPE FLOAT USING REPLACE(original_price, ',', '')::float,
ALTER COLUMN reviews TYPE INT USING REPLACE(reviews, ',', '')::int;

DROP VIEW tracker_data.vw_trackers;
	
CREATE VIEW tracker_data.vw_trackers
AS
-- ### step 4.2: trim spaces for brand_name, color, and strap_material
WITH trim_cte AS (
	SELECT
		*,
		TRIM(',' FROM color) AS color_temp	-- remove comma before/end of string
	FROM (
		SELECT
			TRIM(BOTH FROM brand_name) AS brand_name,
			device_type,
			model_name,
			TRIM(BOTH FROM color) AS color,
			selling_price,
			original_price,
			display,
			rating,
			TRIM(BOTH FROM strap_material) AS strap_material,
			average_battery_life,
			reviews
		FROM tracker_data.fitness_trackers_raw
		ORDER BY brand_name, model_name
	) AS trim_comma
),
-- ### step 4.3: split record(s) that contains multi-valued attribute(s) into multiple rows 
split_cte AS (
	SELECT
		brand_name,
		device_type,
		model_name,
		STRING_TO_TABLE(color_temp, ', ') AS color,			-- split into multiple rows
		selling_price,
		original_price,
		display,
		rating,
		strap_material,
		average_battery_life,
		reviews
	FROM trim_cte
	WHERE color_temp LIKE '%, %'
	UNION ALL											-- combine back with records that contain single-valued color attribute
	SELECT
		brand_name,
		device_type,
		model_name,
		color_temp AS color,
		selling_price,
		original_price,
		display,
		rating,
		strap_material,
		average_battery_life,
		reviews
	FROM trim_cte
	WHERE color_temp NOT LIKE '%, %'
),
--- ### Step 4.4: correct spelling for 'color' column (i.e. 'Bluw' -> 'Blue')
rename_cte AS (
	SELECT
		brand_name,
		device_type,
		model_name,
		color_temp AS color,
		selling_price,
		original_price,
		display,
		rating,
		strap_material,
		average_battery_life,
		reviews
	FROM (
		SELECT *,
			CASE
				WHEN color = 'Bluw' THEN 'Blue'
				ELSE color
			END AS color_temp
		FROM split_cte
	) AS rename_color
),
-- ### Step 4.5: Identifying the Most Recent Fitness Tracker Record
-- Problem:
-- Multiple records share the same values for the following attributes:
-- - brand_name
-- - device_type
-- - model_name
-- - color
-- - display
-- - strap_material
--
-- Assumption:
-- The selling_price and original_price are entirely dependent on the attributes listed above.
-- The first record is the most recent record.
recent_pricing_cte AS (
	SELECT *,
		CASE
			WHEN price_rank = 1 THEN 1
			ELSE 0
		END AS is_price_recent
	FROM (
		SELECT *
		FROM (
			SELECT
				brand_name,
				device_type,
				model_name,
				color,
				selling_price,
				original_price,
				display,
				rating,
				strap_material,
				average_battery_life,
				reviews,
				ROW_NUMBER() OVER (PARTITION BY brand_name,
					device_type,
					model_name,
					color,
					display,
					strap_material ORDER BY selling_price, original_price) AS price_rank
			FROM rename_cte
		) AS price_ranking
	) AS recent_price
),
-- ### Step 4.6: create a surrogate key for brand_name, model_name.
-- This later will be used as a primary key in related tables.
key_cte AS (
	SELECT
		*,
		CONCAT('B', DENSE_RANK() OVER (ORDER BY UPPER(brand_name)))::varchar(5) AS brand_name_pk,
		CONCAT('DT', DENSE_RANK() OVER (ORDER BY UPPER(device_type)))::varchar(8) AS device_type_pk,
		CONCAT('M', DENSE_RANK() OVER (ORDER BY UPPER(model_name)))::varchar(8) AS model_name_pk,
		CONCAT('C', DENSE_RANK() OVER (ORDER BY UPPER(color)))::varchar(5) AS color_pk,
		CONCAT('D', DENSE_RANK() OVER (ORDER BY UPPER(display)))::varchar(5) AS display_pk,
		CONCAT('S', DENSE_RANK() OVER (ORDER BY UPPER(strap_material)))::varchar(5) AS strap_pk,
		(ROW_NUMBER() OVER (ORDER BY rating, average_battery_life, reviews))::int AS detail_pk
	FROM recent_pricing_cte
)
SELECT *
FROM key_cte;

SELECT * FROM tracker_data.vw_trackers;

-- step 5: create fitness tracker records-related tables based on the proposed logical data model
-- dim_brands
CREATE TABLE tracker_data.dim_brands (
	brand_id VARCHAR(5) PRIMARY KEY,
	brand_name VARCHAR(200) NOT NULL
);

INSERT INTO tracker_data.dim_brands (brand_id, brand_name)
SELECT DISTINCT brand_name_pk, brand_name
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.dim_brands;

-- dim_models
CREATE TABLE tracker_data.dim_models (
	model_id VARCHAR(8) PRIMARY KEY,
	model_name VARCHAR(200) NOT NULL
);

INSERT INTO tracker_data.dim_models (model_id, model_name)
WITH compare_model_cte AS (
	SELECT
		model_name,
		model_name_pk,
		LEAD(model_name_pk, 1) OVER (ORDER BY model_name_pk) AS next_model_pk
	FROM (
		SELECT  
			model_name_pk,
			model_name
		FROM tracker_data.vw_trackers
		ORDER BY model_name_pk	
	) AS next_model
)
SELECT
	model_name_pk,
	model_name
FROM (
	SELECT
		model_name,
		model_name_pk,
		CASE
			WHEN model_name_pk = next_model_pk THEN 1
			ELSE 0
		END AS is_duplicate
	FROM compare_model_cte
) AS unique_model_names
WHERE is_duplicate = 0;

SELECT * FROM tracker_data.dim_models;

-- brand_models
CREATE TABLE tracker_data.brand_models (
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	model_id VARCHAR(8) REFERENCES tracker_data.dim_models(model_id) ON DELETE RESTRICT,
	PRIMARY KEY (brand_id, model_id)
);

INSERT INTO tracker_data.brand_models (brand_id, model_id)
SELECT DISTINCT brand_name_pk, model_name_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.brand_models;

-- verify the record count between tracker_data.brand_models and tracker_data.vw_trackers
-- SELECT COUNT(*)
-- FROM (
-- 	SELECT *,
-- 		CASE
-- 			WHEN brand_model_pk = next_brand_model THEN 1
-- 			ELSE 0
-- 		END AS is_duplicate
-- 	FROM (
-- 		SELECT
-- 			brand_name_pk,
-- 			model_name_pk,
-- 			brand_model_pk,
-- 			LEAD(brand_model_pk, 1) OVER (ORDER BY brand_model_pk) AS next_brand_model
-- 		FROM (
-- 			SELECT 
-- 				brand_name_pk,
-- 				model_name_pk,
-- 				CONCAT(brand_name_pk, model_name_pk) AS brand_model_pk
-- 			FROM tracker_data.vw_trackers
-- 			ORDER BY brand_model_pk
-- 		) AS is_same_as_next
-- 	) AS duplicate
-- ) AS cnt_duplicate
-- WHERE is_duplicate = 0

-- dim_colors
CREATE TABLE tracker_data.dim_colors (
	color_id VARCHAR(5) PRIMARY KEY,
	color VARCHAR(200) NOT NULL
);

INSERT INTO tracker_data.dim_colors (color_id, color)
WITH compare_color_cte AS (
	SELECT
		color,
		color_pk,
		LEAD(color_pk, 1) OVER (ORDER BY color) AS next_color_pk
	FROM (
		SELECT 
			DISTINCT color, color_pk
		FROM tracker_data.vw_trackers
		ORDER BY color
	) AS next_color
)
SELECT
	color_pk,
	color
FROM compare_color_cte
WHERE color_pk <> next_color_pk
OR next_color_pk IS NULL;

SELECT * FROM tracker_data.dim_colors;

-- model_colors
CREATE TABLE tracker_data.model_colors (
	model_id VARCHAR(8) REFERENCES tracker_data.dim_models(model_id) ON DELETE RESTRICT,
	color_id VARCHAR(5) REFERENCES tracker_data.dim_colors(color_id) ON DELETE RESTRICT,
	PRIMARY KEY (model_id, color_id)
);	

INSERT INTO tracker_data.model_colors (model_id, color_id)
SELECT DISTINCT model_name_pk, color_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.model_colors;

-- verify the record count between tracker_data.model_colors and tracker_data.vw_trackers
-- SELECT COUNT(*) FROM tracker_data.model_colors;

-- SELECT COUNT(*)
-- FROM (
-- 	SELECT *,
-- 		CASE
-- 			WHEN model_color_pk = next_model_color THEN 1
-- 			ELSE 0
-- 		END AS is_duplicate
-- 	FROM (
-- 		SELECT
-- 			model_name_pk,
-- 			color_pk,
-- 			model_color_pk,
-- 			LEAD(model_color_pk, 1) OVER (ORDER BY model_color_pk) AS next_model_color
-- 		FROM (
-- 			SELECT 
-- 				model_name_pk,
-- 				color_pk,
-- 				CONCAT(model_name_pk, color_pk) AS model_color_pk
-- 			FROM tracker_data.vw_trackers
-- 			ORDER BY model_color_pk
-- 		) AS is_same_as_next
-- 	) AS duplicate
-- ) AS cnt_duplicate
-- WHERE is_duplicate = 0

-- dim_strap_materials
CREATE TABLE tracker_data.dim_strap_materials (
	strap_id VARCHAR(5) PRIMARY KEY,
	strap_material VARCHAR(300) NOT NULL
);

INSERT INTO tracker_data.dim_strap_materials (strap_id, strap_material)
SELECT
	strap_pk,
	strap_material
FROM (
	SELECT
		*,
		CASE
			WHEN strap_pk = next_strap THEN 1
			ELSE 0
		END AS is_duplicate
	FROM (
		SELECT
			strap_material,
			strap_pk,
			LEAD(strap_pk, 1) OVER (ORDER BY strap_material) AS next_strap
		FROM tracker_data.vw_trackers
	) AS compare_strap
	ORDER BY strap_material
) AS xxx
WHERE is_duplicate = 0;

SELECT * FROM tracker_data.dim_strap_materials;

-- dim_displays
CREATE TABLE tracker_data.dim_displays (
	display_id VARCHAR(5) PRIMARY KEY,
	display VARCHAR(300) NOT NULL
);

INSERT INTO tracker_data.dim_displays (display_id, display)
SELECT
	display_pk,
	display
FROM (
	SELECT
		*,
		CASE
			WHEN display_pk = next_display THEN 1
			ELSE 0
		END AS is_duplicate
	FROM (
		SELECT
			display,
			display_pk,
			LEAD(display_pk, 1) OVER (ORDER BY display) AS next_display
		FROM tracker_data.vw_trackers
	) AS compare_display
	ORDER BY display
) AS xxx
WHERE is_duplicate = 0;

SELECT * FROM tracker_data.dim_displays;

-- dim_device_types
CREATE TABLE tracker_data.dim_device_types (
	device_type_id VARCHAR(8) PRIMARY KEY,
	device_type VARCHAR(300) NOT NULL
);

INSERT INTO tracker_data.dim_device_types (device_type_id, device_type)
SELECT
	device_type_pk,
	device_type
FROM (
	SELECT
		*,
		CASE
			WHEN device_type_pk = next_device_type THEN 1
			ELSE 0
		END AS is_duplicate
	FROM (
		SELECT
			device_type,
			device_type_pk,
			LEAD(device_type_pk, 1) OVER (ORDER BY device_type) AS next_device_type
		FROM tracker_data.vw_trackers
	) AS compare_device
	ORDER BY device_type
) AS xxx
WHERE is_duplicate = 0;

SELECT * FROM tracker_data.dim_device_types;

-- brand_displays
CREATE TABLE tracker_data.brand_displays (
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	display_id VARCHAR(5) REFERENCES tracker_data.dim_displays(display_id) ON DELETE RESTRICT,
	PRIMARY KEY (display_id, brand_id)
);

INSERT INTO tracker_data.brand_displays (brand_id, display_id)
SELECT DISTINCT brand_name_pk, display_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.brand_displays;
	
-- brand_devicetypes
CREATE TABLE tracker_data.brand_devicetypes (
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	device_type_id VARCHAR(5) REFERENCES tracker_data.dim_device_types(device_type_id) ON DELETE RESTRICT,
	PRIMARY KEY (brand_id, device_type_id)
);

INSERT INTO tracker_data.brand_deviceTypes (brand_id, device_type_id)
SELECT DISTINCT brand_name_pk, device_type_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.brand_devicetypes;
	
-- brand_straps
CREATE TABLE tracker_data.brand_straps (
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	strap_id VARCHAR(5) REFERENCES tracker_data.dim_strap_materials(strap_id) ON DELETE RESTRICT,
	PRIMARY KEY (brand_id, strap_id)
);

INSERT INTO tracker_data.brand_straps (brand_id, strap_id)
SELECT DISTINCT brand_name_pk, strap_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.brand_straps;

-- dim_device_details
CREATE TABLE tracker_data.dim_device_details (
	detail_id INT PRIMARY KEY,
	rating FLOAT,
	average_battery_life INT,
	reviews INT
);

INSERT INTO tracker_data.dim_device_details (
	detail_id,
	rating,
	average_battery_life,
	reviews
)
SELECT
	detail_pk,
	rating,
	average_battery_life,
	reviews
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.dim_device_details;
	
-- fact_pricing
CREATE TABLE tracker_data.fact_pricing (
	pricing_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	model_id VARCHAR(8) REFERENCES tracker_data.dim_models(model_id) ON DELETE RESTRICT,
	color_id VARCHAR(5) REFERENCES tracker_data.dim_colors(color_id) ON DELETE RESTRICT,
	device_type_id VARCHAR(5) REFERENCES tracker_data.dim_device_types(device_type_id) ON DELETE RESTRICT,
	display_id VARCHAR(5) REFERENCES tracker_data.dim_displays(display_id) ON DELETE RESTRICT,
	strap_id VARCHAR(5) REFERENCES tracker_data.dim_strap_materials(strap_id) ON DELETE RESTRICT,
	selling_price FLOAT NOT NULL,
	original_price FLOAT NOT NULL,
	is_effective BOOLEAN NOT NULL
);

INSERT INTO tracker_data.fact_pricing (
	brand_id,
	model_id,
	color_id,
	device_type_id,
	display_id,
	strap_id,
	selling_price,
	original_price,
	is_effective
)
SELECT
	brand_name_pk,
	model_name_pk,
	color_pk,
	device_type_pk,
	display_pk,
	strap_pk,
	selling_price,
	original_price,
	is_price_recent::boolean
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.fact_pricing;
	
-- model_color_details
CREATE TABLE tracker_data.model_color_details (
	color_id VARCHAR(5) REFERENCES tracker_data.dim_colors(color_id) ON DELETE RESTRICT,
	model_id VARCHAR(8) REFERENCES tracker_data.dim_models(model_id) ON DELETE RESTRICT,
	detail_id INT REFERENCES tracker_data.dim_device_details(detail_id) ON DELETE RESTRICT,
	PRIMARY KEY (color_id, model_id, detail_id)
);

INSERT INTO tracker_data.model_color_details (color_id, model_id, detail_id)
SELECT DISTINCT color_pk, model_name_pk, detail_pk
FROM tracker_data.vw_trackers;

SELECT * FROM tracker_data.model_color_details;

-- wearable_devices
CREATE TABLE tracker_data.wearable_devices (
	patient_device_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	brand_id VARCHAR(5) REFERENCES tracker_data.dim_brands(brand_id) ON DELETE RESTRICT,
	device_type_id VARCHAR(5) REFERENCES tracker_data.dim_device_types(device_type_id) ON DELETE RESTRICT,
	model_id VARCHAR(8) REFERENCES tracker_data.dim_models(model_id) ON DELETE RESTRICT
);

INSERT INTO tracker_data.wearable_devices (
	brand_id,
	device_type_id,
	model_id
)
SELECT
	DISTINCT d.brand_id, d.device_type_id, m.model_id
FROM tracker_data.brand_devicetypes d
	INNER JOIN tracker_data.brand_models m
	ON d.brand_id = m.brand_id;

SELECT * FROM tracker_data.wearable_devices;

-- mockup data created for dim_metric_types and fact_health_metrics table
-- dim_metric_types
CREATE TABLE tracker_data.dim_metric_types (
	metric_type_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	metric_name VARCHAR(50) NOT NULL,
	unit VARCHAR(25) NOT NULL
);

SELECT * FROM tracker_data.dim_metric_types;

-- fact_health_metrics
CREATE TABLE tracker_data.fact_health_metrics (
	metric_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	patient_id INT NOT NULL REFERENCES health_data.dim_patients(patient_id) ON DELETE RESTRICT,
	patient_device_id INT NOT NULL REFERENCES tracker_data.wearable_devices(patient_device_id) ON DELETE RESTRICT,
	metric_type_id INT NOT NULL REFERENCES tracker_data.dim_metric_types(metric_type_id) ON DELETE RESTRICT,
	value FLOAT NOT NULL,
	date_timestamp TIMESTAMP
);

INSERT INTO tracker_data.fact_health_metrics (
	patient_id,
	patient_device_id,
	metric_type_id,
	value,
	date_timestamp
)
VALUES
	(1000,103,1,78,'2023-12-10 08:16:00'),
	(827,104,2,250,'2023-01-01 09:00:00'),
	(12,405,3,5432,'2024-07-09 18:15:00'),
	(5,100,4,2.1,'2024-02-14 07:15:00'),
	(783,100,5,45,'2021-10-10 11:23:00'),
	(999,345,6,7.5,'2020-01-16 17:45:00'),
	(657,5,7,3,'2024-03-19 13:01:00'),
	(23,10,7,5,'2024-03-10 18:15:00'),
	(456,32,8,85,'2023-12-10 08:16:00'),
	(324,99,4,3.2,'2025-01-01 19:27:00'),
	(87,234,2,120,'2020-01-16 17:45:00');

SELECT * FROM tracker_data.fact_health_metrics;

-- Show which patient wears which device and what metrics are being captured
SELECT p.first_name,
	p.last_name,
	b.brand_name,
	m.model_name,
	hm.value,
	hm.date_timestamp,
	mt.metric_name,
	mt.unit
FROM health_data.dim_patients p
	INNER JOIN tracker_data.fact_health_metrics hm ON p.patient_id = hm.patient_id
	INNER JOIN tracker_data.dim_metric_types mt ON hm.metric_type_id = mt.metric_type_id
	INNER JOIN tracker_data.wearable_devices wd ON hm.patient_device_id = wd.patient_device_id
	INNER JOIN tracker_data.dim_brands b ON wd.brand_id = b.brand_id
	INNER JOIN tracker_data.dim_models m ON wd.model_id = m.model_id;

------>>>>>> KEEP THE BELOW QUERIES IN CASE ANYTHING HAPPENS. DO NOT DELETE! <<<<<<------

-- SELECT
-- 	model_name,
-- 	SPLIT_PART(model_name, 'GPS', 1) AS name_of_model,
-- 	SUBSTRING(model_name from 'GPS \+ Cellular') AS connectivity,
-- 	SUBSTRING(model_name, 'Cellular[,\-]* ([0-9]+ mm)') AS case_size,
--     -- Extract case material (ensuring "Gray Ceramic" becomes just "Ceramic")
--     CASE 
--         WHEN model_name ~* 'Aluminium' THEN 'Aluminium'
--         WHEN model_name ~* 'Stainless Steel' THEN 'Stainless Steel'
--         WHEN model_name ~* 'Ceramic' THEN 'Ceramic'
--         ELSE NULL
--     END AS case_material,
  
--     -- Extract case color (ensuring "Gray Ceramic" → "Gray")
--     CASE 
--         WHEN model_name ~* 'Gray Ceramic' THEN 'Gray'
--         WHEN model_name ~* 'White Ceramic' THEN 'White'
--         WHEN model_name ~* 'Space Grey' THEN 'Space Grey'
-- 		WHEN model_name ~* 'Space Black' THEN 'Space Black'
--         WHEN model_name ~* 'Graphite' THEN 'Graphite'
--         WHEN model_name ~* 'Gold' THEN 'Gold'
--         WHEN model_name ~* 'Silver' THEN 'Silver'
--         WHEN model_name ~* 'Red' THEN 'Red'
--         WHEN model_name ~* 'Blue' THEN 'Blue'
--         ELSE NULL
--     END AS case_color
-- FROM tracker_data.fitness_trackers_raw
-- WHERE model_name LIKE '% + %'; -- model_name LIKE '%,%' also covered;

-- SELECT
-- 	model_name,
-- 	SPLIT_PART(model_name, 'GPS', 1) AS name_of_model,
-- 	SUBSTRING(model_name, '(\bGPS\b)?') AS connectivity,
-- 	SUBSTRING(model_name, '-\s?([0-9]+ mm)') AS case_size,
--     -- Extract case material (ensuring "Gray Ceramic" becomes just "Ceramic")
--     CASE 
--         WHEN model_name ~* 'Aluminium' THEN 'Aluminium'
--         WHEN model_name ~* 'Stainless Steel' THEN 'Stainless Steel'
--         WHEN model_name ~* 'Ceramic' THEN 'Ceramic'
--         ELSE NULL
--     END AS case_material,
  
--     -- Extract case color (ensuring "Gray Ceramic" → "Gray")
--     CASE 
--         WHEN model_name ~* 'Gray Ceramic' THEN 'Gray'
--         WHEN model_name ~* 'White Ceramic' THEN 'White'
--         WHEN model_name ~* 'Space Grey' THEN 'Space Grey'
-- 		WHEN model_name ~* 'Space Black' THEN 'Space Black'
--         WHEN model_name ~* 'Graphite' THEN 'Graphite'
--         WHEN model_name ~* 'Gold' THEN 'Gold'
--         WHEN model_name ~* 'Silver' THEN 'Silver'
--         WHEN model_name ~* 'Red' THEN 'Red'
--         WHEN model_name ~* 'Blue' THEN 'Blue'
--         ELSE NULL
--     END AS case_color
-- FROM tracker_data.fitness_trackers_raw
-- WHERE tracker_id IN (
-- 	355,
-- 	357,
-- 	381,
-- 	382,
-- 	383,
-- 	430,
-- 	431,
-- 	433,
-- 	434,
-- 	435,
-- 	436,
-- 	439,
-- 	440
-- );


