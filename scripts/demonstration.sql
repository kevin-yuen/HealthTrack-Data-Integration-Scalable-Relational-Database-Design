-- M:N between patients and allergies
SELECT allergies,
	COUNT(*)
FROM health_data.medical_records_raw
GROUP BY allergies;

----------------------------------------------------------------------------------
--------------------- BELOW DROPS ARE FOR DEMO PURPOSES ONLY ---------------------
----------------------------------------------------------------------------------
DROP VIEW IF EXISTS health_data.vw_patients;
DROP TABLE IF EXISTS health_data.medical_records_raw;
DROP TABLE IF EXISTS health_data.allergies;
DROP TABLE IF EXISTS health_data.dim_allergy_types;
DROP TABLE IF EXISTS health_data.medical_conditions;
DROP TABLE IF EXISTS health_data.dim_medical_condition_types;
DROP TABLE IF EXISTS health_data.medications;
DROP TABLE IF EXISTS health_data.medical_records;
DROP MATERIALIZED VIEW IF EXISTS tracker_data.vw_patient_devices;
DROP TABLE IF EXISTS tracker_data.fact_health_metrics;
DROP TABLE IF EXISTS health_data.dim_patients;

DROP TABLE IF EXISTS tracker_data.brand_devicetypes;
DROP TABLE IF EXISTS tracker_data.brand_displays;
DROP TABLE IF EXISTS tracker_data.brand_models;
DROP MATERIALIZED VIEW IF EXISTS tracker_data.vw_brand_straps;
DROP TABLE IF EXISTS tracker_data.brand_straps;
DROP MATERIALIZED VIEW IF EXISTS tracker_data.vw_product_pricing;
DROP TABLE IF EXISTS tracker_data.fact_pricing;
DROP TABLE IF EXISTS tracker_data.wearable_devices;
DROP TABLE IF EXISTS tracker_data.dim_brands;
DROP TABLE IF EXISTS tracker_data.model_colors;
DROP MATERIALIZED VIEW IF EXISTS tracker_data.vw_model_colors;
DROP TABLE IF EXISTS tracker_data.model_color_details;
DROP TABLE IF EXISTS tracker_data.dim_colors;
DROP TABLE IF EXISTS tracker_data.dim_device_details;
DROP TABLE IF EXISTS tracker_data.dim_device_types;
DROP TABLE IF EXISTS tracker_data.dim_displays;
DROP TABLE IF EXISTS tracker_data.dim_metric_types;
DROP TABLE IF EXISTS tracker_data.dim_models;
DROP TABLE IF EXISTS tracker_data.dim_strap_materials;
DROP VIEW IF EXISTS tracker_data.vw_trackers;
DROP TABLE IF EXISTS tracker_data.fitness_trackers_raw;