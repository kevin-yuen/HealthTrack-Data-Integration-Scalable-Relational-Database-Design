-- create table for data import
CREATE TABLE d597.medical_records (
	patient_id INT PRIMARY KEY,
	name VARCHAR(500),
	date_of_birth DATE,
	gender VARCHAR(100),
	medical_conditions VARCHAR(500),
	medications VARCHAR(500),
	allergies VARCHAR(500),
	last_appointment_date DATE,
	tracker VARCHAR(500)
)

SELECT * FROM d597.medical_records;

-- check for nullable columns
SELECT *
FROM d597.medical_records
WHERE (patient_id IS NULL)
	OR (name IS NULL)
	OR (date_of_birth IS NULL)
	OR (gender IS NULL)
	OR (medical_conditions IS NULL)
	OR (medications IS NULL)
	OR (allergies IS NULL)
	OR (last_appointment_date IS NULL)
	OR (tracker IS NULL);

-- total number of records
SELECT COUNT(*) FROM d597.medical_records;		-- 100,000

SELECT
	COUNT(DISTINCT(patient_id)) AS cnt_id,		-- only patient_id has unique values
	COUNT(DISTINCT(name)) AS cnt_name,
	COUNT(DISTINCT(date_of_birth)) AS cnt_dob,
	COUNT(DISTINCT(gender)) AS cnt_gender,
	COUNT(DISTINCT(medical_conditions)) AS cnt_med_conditions,
	COUNT(DISTINCT(medications)) AS cnt_medications,
	COUNT(DISTINCT(allergies)) AS cnt_allergies,
	COUNT(DISTINCT(last_appointment_date)) AS cnt_last_appt_dt,
	COUNT(DISTINCT(tracker)) AS cnt_tracker
FROM d597.medical_records;

-------->>>>>> get patients with the (same name) OR (same name AND dob) OR (same name AND dob AND gender) <<<<<<--------
-- get patients with the same name
SELECT name,
	COUNT(name) AS cnt_name
FROM d597.medical_records
GROUP BY name
HAVING COUNT(name) > 1;			-- 14,845 records	

-- get patients with the same name AND dob
SELECT name,
	date_of_birth,
	COUNT(name) AS cnt_name
FROM d597.medical_records
GROUP BY name,
	date_of_birth
HAVING COUNT(name) > 1;			-- 2 records

-- get patients with the same name AND dob AND gender
SELECT name,
	date_of_birth,
	gender,
	COUNT(name) AS cnt_name
FROM d597.medical_records
GROUP BY name,
	date_of_birth,
	gender
HAVING COUNT(name) > 1;			-- no record

SELECT *
FROM d597.medical_records
WHERE name = 'Brian Harris';

-------->>>>>> check for multi-valued attributes <<<<<<--------
SELECT *
FROM d597.medical_records
WHERE name LIKE '%,%';			-- no record

SELECT DISTINCT(gender)
FROM d597.medical_records;		-- either 'M' or 'F'

SELECT DISTINCT(medications)
FROM d597.medical_records;		-- either 'No' or 'Yes'

SELECT DISTINCT(medical_conditions)
FROM d597.medical_records;		-- either 'Watch', 'None', or 'Mild'

SELECT *
FROM d597.medical_records
WHERE allergies LIKE '%,%';		-- 3000 records have 'mold, fungus'

SELECT DISTINCT(allergies)
FROM d597.medical_records;
-- "peanut"
-- "pet dander"
-- "dietary"
-- "plant"
-- "mold, fungus"
-- "egg"
-- "skin"
-- "animal"
-- "None"

SELECT *
FROM d597.medical_records
WHERE tracker LIKE '%,%';		-- no record

-------->>>>>> check for string value with spaces before and/or after across all columns <<<<<<--------
SELECT *
FROM d597.medical_records
WHERE (name LIKE ' %')
OR (name LIKE '% ')
OR (name LIKE ' % ');						-- no record

SELECT *
FROM d597.medical_records
WHERE (medical_conditions LIKE ' %')
OR (medical_conditions LIKE '% ')
OR (medical_conditions LIKE ' % ');		-- no record

SELECT *
FROM d597.medical_records
WHERE (medications LIKE ' %')
OR (medications LIKE '% ')
OR (medications LIKE ' % ');				-- no record

SELECT *
FROM d597.medical_records
WHERE (allergies LIKE ' %')
OR (allergies LIKE '% ')
OR (allergies LIKE ' % ');					-- no record

SELECT *
FROM d597.medical_records
WHERE (tracker LIKE ' %')
OR (tracker LIKE '% ')
OR (tracker LIKE ' % ');					-- no record

-------->>>>>> check for null values <<<<<<--------
SELECT *
FROM d597.medical_records
WHERE gender IS NOT NULL;
