DROP SCHEMA IF EXISTS health_data;

CREATE SCHEMA IF NOT EXISTS health_data;

-- step 1: create health_data.medical_records_raw for storing raw data
DROP TABLE IF EXISTS health_data.medical_records_raw;

CREATE TABLE IF NOT EXISTS health_data.medical_records_raw (
	patient_id VARCHAR(100) NOT NULL,
	name VARCHAR(200) NOT NULL,
	date_of_birth VARCHAR(100) NOT NULL,
	gender VARCHAR(10) NOT NULL,
	medical_conditions VARCHAR(100) NOT NULL,
	medications VARCHAR(200) NOT NULL,
	allergies VARCHAR(100) NOT NULL,
	last_appointment_date VARCHAR(100) NOT NULL,
	tracker VARCHAR(200) NOT NULL
);

-- step 2: import raw data from csv file to health_data.medical_records_raw table (via 'Import/Export Data' function)

-- step 3: verify that data has sucessfully ingested
SELECT * FROM health_data.medical_records_raw;

-- step 4: transformation - clean up data
-- ### step 4.1: cast to appropriate data type for each column
ALTER TABLE health_data.medical_records_raw
ALTER COLUMN patient_id TYPE INT USING patient_id::integer,
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
ALTER COLUMN gender TYPE CHAR USING gender::char,
ALTER COLUMN medications TYPE BOOLEAN USING medications::boolean,
ALTER COLUMN last_appointment_date TYPE DATE USING last_appointment_date::date;

SELECT * FROM health_data.medical_records_raw;

-- ### step 4.2:
-- for allergies column, if a record contains a comma, split it into multiple rows
-- split name into title, first_name, last_name, suffix, and degree column
DROP VIEW IF EXISTS health_data.vw_patients;

CREATE VIEW health_data.vw_patients
AS
WITH allergies_cte AS (
-- split records that contain a comma into multiple rows,
-- then combine with records that don't contain a comma
	SELECT
		patient_id,
		name,
		date_of_birth,
		gender,
		medical_conditions,
		medications,
		allergies,
		last_appointment_date,
		tracker
	FROM health_data.medical_records_raw
	WHERE allergies NOT LIKE '%,%'
	UNION
	SELECT
		patient_id,
		name,
		date_of_birth,
		gender,
		medical_conditions,
		medications,
		STRING_TO_TABLE(allergies, ', ') AS allergies,		-- split into multiple rows
		last_appointment_date,
		tracker
	FROM health_data.medical_records_raw
	WHERE allergies LIKE '%,%'
)
SELECT
	patient_id,
	------------- name split logic starts here -------------
	CASE
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND
			temp_name[1] IN ('Mr.', 'Dr.', 'Ms.', 'Mrs.', 'Miss') THEN temp_name[1]::varchar(50)
		WHEN ARRAY_LENGTH(temp_name, 1) = 4 THEN temp_name[1]::varchar(50)
	END AS title,
	CASE
		WHEN ARRAY_LENGTH(temp_name, 1) = 2 THEN temp_name[1]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND (
				temp_name[1] <> 'Mr.' AND
				temp_name[1] <> 'Dr.' AND
				temp_name[1] <> 'Ms.' AND
				temp_name[1] <> 'Mrs.' AND
				temp_name[1] <> 'Miss'
			) THEN temp_name[1]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND
			temp_name[1] IN ('Mr.', 'Dr.', 'Ms.', 'Mrs.', 'Miss') THEN temp_name[2]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 4 THEN temp_name[2]::varchar(100)
	END AS first_name,
	CASE
		WHEN ARRAY_LENGTH(temp_name, 1) = 2 THEN temp_name[2]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND (
				temp_name[1] <> 'Mr.' AND
				temp_name[1] <> 'Dr.' AND
				temp_name[1] <> 'Ms.' AND
				temp_name[1] <> 'Mrs.' AND
				temp_name[1] <> 'Miss'
			) THEN temp_name[2]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND
			temp_name[1] IN ('Mr.', 'Dr.', 'Ms.', 'Mrs.', 'Miss') THEN temp_name[3]::varchar(100)
		WHEN ARRAY_LENGTH(temp_name, 1) = 4 THEN temp_name[3]::varchar(100)
	END AS last_name,
	CASE
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND
			temp_name[3] IN ('V', 'Jr.', 'II', 'III', 'IV') THEN temp_name[3]::varchar(50)
		WHEN ARRAY_LENGTH(temp_name, 1) = 4 AND
			temp_name[4] IN ('V', 'Jr.', 'II', 'III', 'IV') THEN temp_name[4]::varchar(50)
	END AS suffix,
	CASE
		WHEN ARRAY_LENGTH(temp_name, 1) = 3 AND
			temp_name[3] IN ('DVM', 'PhD', 'DDS', 'MD') THEN temp_name[3]::varchar(50)
		WHEN ARRAY_LENGTH(temp_name, 1) = 4 AND 
			temp_name[4] IN ('DVM', 'PhD', 'DDS', 'MD') THEN temp_name[4]::varchar(50)
	END AS degree,
	------------- name split logic ends here -------------
	date_of_birth,
	gender,
	medical_conditions,
	medications,
	allergies,
	last_appointment_date,
	tracker
FROM (
	SELECT
		*,
		STRING_TO_ARRAY(name, ' ') AS temp_name
	FROM allergies_cte
)
AS name_split;

SELECT * FROM health_data.vw_patients;
SELECT DISTINCT(allergies) FROM health_data.vw_patients;

-- step 5: create medical records-related tables based on the proposed logical data model
CREATE TABLE IF NOT EXISTS health_data.dim_patients (
	patient_id INT PRIMARY KEY,
	title VARCHAR(10),
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	suffix VARCHAR(10),
	degree VARCHAR(10),
	date_of_birth DATE NOT NULL,
	gender CHAR(1) NOT NULL
);

-- medication_taken, condition_name, and allergy_name are temporary columns for inserting data to child tables
-- these columns will be removed after data insertion
CREATE TABLE IF NOT EXISTS health_data.medical_records (
	record_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	patient_id INT REFERENCES health_data.dim_patients (patient_id) ON DELETE CASCADE,
	last_appointment_date DATE NOT NULL,
	medication_taken BOOLEAN NOT NULL,
	condition_name VARCHAR(100) NOT NULL,
	allergy_name VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS health_data.medications (
	medication_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	record_id INT REFERENCES health_data.medical_records (record_id) ON DELETE CASCADE,
	medication_taken BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS health_data.dim_medical_condition_types (
	condition_type_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	condition_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS health_data.medical_conditions (
	condition_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	record_id INT REFERENCES health_data.medical_records (record_id) ON DELETE CASCADE,
	condition_type_id INT REFERENCES health_data.dim_medical_condition_types (condition_type_id)
);

CREATE TABLE IF NOT EXISTS health_data.dim_allergy_types (
	allergy_type_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	allergy_name VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS health_data.allergies (
	allergy_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
		(START WITH 1),
	record_id INT REFERENCES health_data.medical_records (record_id) ON DELETE CASCADE,
	allergy_type_id INT REFERENCES health_data.dim_allergy_types (allergy_type_id)
);

-- step 6: load data into data sinks
-- 1. dim_patients
INSERT INTO health_data.dim_patients (
	patient_id,
	title,
	first_name,
	last_name,
	suffix,
	degree,
	date_of_birth,
	gender
)
SELECT
	DISTINCT(patient_id),
	title,
	first_name,
	last_name,
	suffix,
	degree,
	date_of_birth,
	gender
FROM health_data.vw_patients
ORDER BY patient_id;
	
-- 2. medical_records
INSERT INTO health_data.medical_records (
	patient_id,
	last_appointment_date,
	medication_taken,
	condition_name,
	allergy_name
)
SELECT
	patient_id,
	last_appointment_date,
	medications,
	medical_conditions,
	allergies
FROM health_data.vw_patients;

-- 3. dim_allergy_types
INSERT INTO health_data.dim_allergy_types (allergy_name)
SELECT DISTINCT(allergy_name)
FROM health_data.medical_records;

-- 4. allergies
INSERT INTO health_data.allergies (record_id, allergy_type_id)
SELECT
	record_id,
	allergy_type_id
FROM health_data.medical_records m
	INNER JOIN health_data.dim_allergy_types a
	ON m.allergy_name = a.allergy_name;

-- 5. dim_medical_condition_types
INSERT INTO health_data.dim_medical_condition_types (condition_name)
SELECT
	DISTINCT(condition_name)
FROM health_data.medical_records;

-- 6. medical_conditions
INSERT INTO health_data.medical_conditions (record_id, condition_type_id)
SELECT
	record_id,
	condition_type_id
FROM health_data.medical_records m
	INNER JOIN health_data.dim_medical_condition_types mc
	ON m.condition_name = mc.condition_name;

-- 7. medications
INSERT INTO health_data.medications (record_id, medication_taken)
SELECT
	record_id,
	medication_taken
FROM health_data.medical_records;

-- drop medication_taken, condition_name, and allergy_name from medical_records as they are temporary columns for inserting data into its child tables
ALTER TABLE health_data.medical_records
DROP COLUMN medication_taken,
DROP COLUMN condition_name,
DROP COLUMN allergy_name;

-- verify data loaded successfully
SELECT * FROM health_data.dim_patients;
SELECT * FROM health_data.dim_allergy_types;
SELECT * FROM health_data.dim_medical_condition_types;
SELECT * FROM health_data.allergies;
SELECT * FROM health_data.medications;
SELECT * FROM health_data.medical_conditions;
SELECT * FROM health_data.medical_records;

