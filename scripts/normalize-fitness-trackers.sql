-- CREATE SCHEMA d597;

-- DROP TABLE IF EXISTS d597.fitness_tracker;

-- CREATE TABLE d597.fitness_tracker (
-- 	brand_name VARCHAR(500),
-- 	device_type VARCHAR(500),
-- 	model_name VARCHAR(500),
-- 	color VARCHAR(500),
-- 	selling_price TEXT,
-- 	original_price TEXT,
-- 	display VARCHAR(500),
-- 	rating DECIMAL(2,1),
-- 	strap_material VARCHAR(500),
-- 	average_battery_life TEXT,
-- 	reviews TEXT
-- );

SELECT *
FROM d597.fitness_tracker;

-- get total number of records
SELECT COUNT(*)
FROM d597.fitness_tracker;		-- total 565 records

-- get count of distinct values for each column
SELECT
	COUNT(DISTINCT(brand_name)) AS cnt_of_unique_brand_name,
	COUNT(DISTINCT(device_type)) AS cnt_of_unique_device_type,
	COUNT(DISTINCT(model_name)) AS cnt_of_unique_model_name,
	COUNT(DISTINCT(color)) AS cnt_of_unique_color,
	COUNT(DISTINCT(selling_price)) AS cnt_of_unique_selling_price,
	COUNT(DISTINCT(original_price)) AS cnt_of_unique_original_price,
	COUNT(DISTINCT(display)) AS cnt_of_unique_display,
	COUNT(DISTINCT(rating)) AS cnt_of_unique_rating,
	COUNT(DISTINCT(strap_material)) AS cnt_of_unique_strap_material,
	COUNT(DISTINCT(average_battery_life)) AS cnt_of_unique_avg_battery_life,
	COUNT(DISTINCT(reviews)) AS cnt_of_unique_reviews
FROM d597.fitness_tracker;		-- no unique value across all columns

------>>>>>> construct primary key <<<<<<------
-- construct surrogate key as a primary key due to the following reasons:
-- 1. no unique value across multiple records for brand_name / device_type / model_name / color
-- 2. brand_name + device_type + model_name != unique key
-- 3. brand_name + device_type + model_name + color != unique key

-- brand_name + device_type + model_name != unique key
With concat AS (
	SELECT 
		*,
		CONCAT(brand_name, device_type, new_model_name) AS concat_attributes
	FROM (
		SELECT brand_name,
			device_type,
			model_name,
			REPLACE(model_name, ' ', '') AS new_model_name
		FROM d597.fitness_tracker
	) AS concat_attributes
)
SELECT
	brand_name,
	device_type,
	model_name,
	concat_attributes,
	COUNT(concat_attributes) AS cnt_of_unique_key
FROM concat
GROUP BY
	brand_name,
	device_type,
	model_name,
	concat_attributes
HAVING COUNT(concat_attributes) > 1;

SELECT *
FROM d597.fitness_tracker
WHERE brand_name = 'FOSSIL '
	AND device_type = 'Smartwatch'
	AND model_name = 'Jacqueline Hybrid';

-- brand_name + device_type + model_name + color != unique key
With concat AS (
	SELECT 
		*,
		CONCAT(brand_name, device_type, color, new_model_name) AS concat_attributes
	FROM (
		SELECT brand_name,
			device_type,
			model_name,
			color,
			REPLACE(model_name, ' ', '') AS new_model_name
		FROM d597.fitness_tracker
	) AS concat_attributes
)
SELECT
	brand_name,
	device_type,
	model_name,
	color,
	concat_attributes,
	COUNT(concat_attributes) AS cnt_of_unique_key
FROM concat
GROUP BY
	brand_name,
	device_type,
	model_name,
	color,
	concat_attributes
HAVING COUNT(concat_attributes) > 1;

-- only selling_price and original_price differ between both records
SELECT *
FROM d597.fitness_tracker
WHERE model_name = 'Galaxy Watch 3';

------>>>>>> check whether any spaces before or after the string across all columns <<<<<<------
-- result: brand_name, color, strap_material have spaces before and/or after the string
SELECT *
FROM d597.fitness_tracker
WHERE (brand_name LIKE ' % ')
	OR (brand_name LIKE '% ')
	OR (brand_name LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (device_type LIKE ' % ')
	OR (device_type LIKE '% ')
	OR (device_type LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (model_name LIKE ' % ')
	OR (model_name LIKE '% ')
	OR (model_name LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (color LIKE ' % ')
	OR (color LIKE '% ')
	OR (color LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (selling_price LIKE ' % ')
	OR (selling_price LIKE '% ')
	OR (selling_price LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (original_price LIKE ' % ')
	OR (original_price LIKE '% ')
	OR (original_price LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (display LIKE ' % ')
	OR (display LIKE '% ')
	OR (display LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (strap_material LIKE ' % ')
	OR (strap_material LIKE '% ')
	OR (strap_material LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (average_battery_life LIKE ' % ')
	OR (average_battery_life LIKE '% ')
	OR (average_battery_life LIKE ' %');

SELECT *
FROM d597.fitness_tracker
WHERE (reviews LIKE ' % ')
	OR (reviews LIKE '% ')
	OR (reviews LIKE ' %');

------>>>>>> distinct value <<<<<<------
SELECT DISTINCT(brand_name) AS brand_name
FROM d597.fitness_tracker
ORDER BY brand_name;

SELECT DISTINCT(model_name) AS model_name
FROM d597.fitness_tracker
ORDER BY model_name;		-- data inconsistency (e.g. "Q Machine" vs "Q MACHINE")

SELECT DISTINCT(device_type) AS device_type
FROM d597.fitness_tracker
ORDER BY device_type;

SELECT DISTINCT(color) AS color
FROM d597.fitness_tracker
ORDER BY color;				-- data inconsistency (e.g. "black" vs "Black", "White" vs "White ")
							-- multi-valued (e.g. "Storm Blue, Black, Rosewood")

SELECT DISTINCT(display) AS display
FROM d597.fitness_tracker
ORDER BY display;

SELECT DISTINCT(strap_material) AS display
FROM d597.fitness_tracker
ORDER BY display;			-- data inconsistency (e.g. "Leather" vs "Leather ")

-- confirm fitness_tracker.model_name = medical_records.tracker
SELECT *
FROM (
	SELECT tr.brand_name,
		tr.device_type,
		tr.model_name,
		mr.tracker
	FROM d597.medical_records mr
		INNER JOIN d597.fitness_tracker tr
		ON mr.tracker = tr.model_name
) AS devices
WHERE model_name = tracker;

SELECT brand_name,
	COUNT(strap_material)
FROM d597.fitness_tracker
GROUP BY brand_name;
