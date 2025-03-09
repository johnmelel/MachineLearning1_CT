DROP TABLE IF EXISTS ml1_project.admissions;
CREATE TABLE ml1_project.admissions (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    admittime VARCHAR(50),
    dischtime VARCHAR(50),
    deathtime VARCHAR(50),
    admission_type VARCHAR(50),
    admit_provider_id VARCHAR(50),
    admission_location VARCHAR(100),
    discharge_location VARCHAR(100),
    insurance VARCHAR(100),
    language VARCHAR(50),
    marital_status VARCHAR(50),
    race VARCHAR(100),
    edregtime VARCHAR(50),
    edouttime VARCHAR(50),
    hospital_expire_flag VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/admissions.csv'
INTO TABLE ml1_project.admissions
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.diagnoses_icd;
CREATE TABLE ml1_project.diagnoses_icd (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    seq_num VARCHAR(10),
    icd_code VARCHAR(50),
    icd_version VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/diagnoses_icd.csv'
INTO TABLE ml1_project.diagnoses_icd
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.discharge;
CREATE TABLE ml1_project.discharge (
    note_id VARCHAR(50),
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    note_type VARCHAR(50),
    note_seq VARCHAR(20),
    charttime VARCHAR(50),
    storetime VARCHAR(50),
    text TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/discharge.csv'
INTO TABLE ml1_project.discharge
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.discharge_detail;
CREATE TABLE ml1_project.discharge_detail (
    note_id VARCHAR(50),
    subject_id VARCHAR(20),
    field_name VARCHAR(100),
    field_value TEXT,
    field_ordinal VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/discharge_detail.csv'
INTO TABLE ml1_project.discharge_detail
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.drgcodes;
CREATE TABLE ml1_project.drgcodes (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    drg_type VARCHAR(50),
    drg_code VARCHAR(20),
    description TEXT,
    drg_severity VARCHAR(10),
    drg_mortality VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/drgcodes.csv'
INTO TABLE ml1_project.drgcodes
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.d_hcpcs;
CREATE TABLE ml1_project.d_hcpcs (
    code VARCHAR(50),
    category VARCHAR(10),
    long_description TEXT,
    short_description VARCHAR(100)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/d_hcpcs.csv'
INTO TABLE ml1_project.d_hcpcs
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.d_icd_diagnoses;
CREATE TABLE ml1_project.d_icd_diagnoses (
    icd_code VARCHAR(50),
    icd_version VARCHAR(10),
    long_title TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/d_icd_diagnoses.csv'
INTO TABLE ml1_project.d_icd_diagnoses
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.d_icd_procedures;
CREATE TABLE ml1_project.d_icd_procedures (
    icd_code VARCHAR(50),
    icd_version VARCHAR(10),
    long_title TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/d_icd_procedures.csv'
INTO TABLE ml1_project.d_icd_procedures
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.d_labitems;
CREATE TABLE ml1_project.d_labitems (
    itemid VARCHAR(20),
    label VARCHAR(100),
    fluid VARCHAR(50),
    category VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/d_labitems.csv'
INTO TABLE ml1_project.d_labitems
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.emar;
CREATE TABLE ml1_project.emar (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    emar_id VARCHAR(50),
    emar_seq VARCHAR(20),
    poe_id VARCHAR(50),
    pharmacy_id VARCHAR(20),
    enter_provider_id VARCHAR(50),
    charttime VARCHAR(50),
    medication VARCHAR(100),
    event_txt TEXT,
    scheduletime VARCHAR(50),
    storetime VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/emar.csv'
INTO TABLE ml1_project.emar
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.emar_detail;
CREATE TABLE ml1_project.emar_detail (
    subject_id VARCHAR(20),
    emar_id VARCHAR(50),
    emar_seq VARCHAR(20),
    parent_field_ordinal VARCHAR(20),
    administration_type VARCHAR(50),
    pharmacy_id VARCHAR(20),
    barcode_type VARCHAR(50),
    reason_for_no_barcode TEXT,
    complete_dose_not_given VARCHAR(50),
    dose_due VARCHAR(50),
    dose_due_unit VARCHAR(50),
    dose_given VARCHAR(50),
    dose_given_unit VARCHAR(50),
    will_remainder_of_dose_be_given VARCHAR(50),
    product_amount_given VARCHAR(50),
    product_unit VARCHAR(50),
    product_code VARCHAR(50),
    product_description TEXT,
    product_description_other TEXT,
    prior_infusion_rate VARCHAR(50),
    infusion_rate VARCHAR(50),
    infusion_rate_adjustment VARCHAR(50),
    infusion_rate_adjustment_amount VARCHAR(50),
    infusion_rate_unit VARCHAR(50),
    route VARCHAR(50),
    infusion_complete VARCHAR(50),
    completion_interval VARCHAR(50),
    new_iv_bag_hung VARCHAR(50),
    continued_infusion_in_other_location VARCHAR(50),
    restart_interval VARCHAR(50),
    side VARCHAR(50),
    site VARCHAR(50),
    non_formulary_visual_verification VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/emar_detail.csv'
INTO TABLE ml1_project.emar_detail
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.hcpcsevents;
CREATE TABLE ml1_project.hcpcsevents (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    chartdate VARCHAR(50),
    hcpcs_cd VARCHAR(50),
    seq_num VARCHAR(10),
    short_description TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/hcpcsevents.csv'
INTO TABLE ml1_project.hcpcsevents
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.labevents;
CREATE TABLE ml1_project.labevents (
    labevent_id VARCHAR(20),
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    specimen_id VARCHAR(20),
    itemid VARCHAR(20),
    order_provider_id VARCHAR(50),
    charttime VARCHAR(50),
    storetime VARCHAR(50),
    value TEXT,
    valuenum VARCHAR(20),
    valueuom VARCHAR(50),
    ref_range_lower VARCHAR(20),
    ref_range_upper VARCHAR(20),
    flag VARCHAR(50),
    priority VARCHAR(50),
    comments TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/labevents.csv'
INTO TABLE ml1_project.labevents
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.microbiologyevents;
CREATE TABLE ml1_project.microbiologyevents (
    microevent_id VARCHAR(20),
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    micro_specimen_id VARCHAR(20),
    order_provider_id VARCHAR(50),
    chartdate VARCHAR(50),
    charttime VARCHAR(50),
    spec_itemid VARCHAR(20),
    spec_type_desc VARCHAR(100),
    test_seq VARCHAR(20),
    storedate VARCHAR(50),
    storetime VARCHAR(50),
    test_itemid VARCHAR(20),
    test_name VARCHAR(100),
    org_itemid VARCHAR(20),
    org_name VARCHAR(100),
    isolate_num VARCHAR(20),
    quantity VARCHAR(50),
    ab_itemid VARCHAR(20),
    ab_name VARCHAR(100),
    dilution_text VARCHAR(50),
    dilution_comparison VARCHAR(50),
    dilution_value VARCHAR(20),
    interpretation VARCHAR(50),
    comments TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/microbiologyevents.csv'
INTO TABLE ml1_project.microbiologyevents
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.omr;
CREATE TABLE ml1_project.omr (
    subject_id VARCHAR(20),
    chartdate VARCHAR(50),
    seq_num VARCHAR(10),
    result_name VARCHAR(100),
    result_value TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/omr.csv'
INTO TABLE ml1_project.omr
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.patients;
CREATE TABLE ml1_project.patients (
    subject_id VARCHAR(20),
    gender VARCHAR(10),
    anchor_age VARCHAR(10),
    anchor_year VARCHAR(10),
    anchor_year_group VARCHAR(50),
    dod VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/patients.csv'
INTO TABLE ml1_project.patients
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.pharmacy;
CREATE TABLE ml1_project.pharmacy (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    pharmacy_id VARCHAR(20),
    poe_id VARCHAR(50),
    starttime VARCHAR(50),
    stoptime VARCHAR(50),
    medication VARCHAR(100),
    proc_type VARCHAR(50),
    status VARCHAR(50),
    entertime VARCHAR(50),
    verifiedtime VARCHAR(50),
    route VARCHAR(50),
    frequency VARCHAR(50),
    disp_sched VARCHAR(50),
    infusion_type VARCHAR(50),
    sliding_scale VARCHAR(50),
    lockout_interval VARCHAR(50),
    basal_rate VARCHAR(20),
    one_hr_max VARCHAR(50),
    doses_per_24_hrs VARCHAR(10),
    duration VARCHAR(20),
    duration_interval VARCHAR(50),
    expiration_value VARCHAR(10),
    expiration_unit VARCHAR(50),
    expirationdate VARCHAR(50),
    dispensation VARCHAR(50),
    fill_quantity VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/pharmacy.csv'
INTO TABLE ml1_project.pharmacy
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.prescriptions;
CREATE TABLE ml1_project.prescriptions (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    pharmacy_id VARCHAR(20),
    poe_id VARCHAR(50),
    poe_seq VARCHAR(20),
    order_provider_id VARCHAR(50),
    starttime VARCHAR(50),
    stoptime VARCHAR(50),
    drug_type VARCHAR(50),
    drug VARCHAR(100),
    formulary_drug_cd VARCHAR(50),
    gsn VARCHAR(50),
    ndc VARCHAR(50),
    prod_strength VARCHAR(50),
    form_rx VARCHAR(50),
    dose_val_rx VARCHAR(50),
    dose_unit_rx VARCHAR(50),
    form_val_disp VARCHAR(50),
    form_unit_disp VARCHAR(50),
    doses_per_24_hrs VARCHAR(10),
    route VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/prescriptions.csv'
INTO TABLE ml1_project.prescriptions
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.procedures_icd;
CREATE TABLE ml1_project.procedures_icd (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    seq_num VARCHAR(10),
    chartdate VARCHAR(50),
    icd_code VARCHAR(50),
    icd_version VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/procedures_icd.csv'
INTO TABLE ml1_project.procedures_icd
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.radiology;
CREATE TABLE ml1_project.radiology (
    note_id VARCHAR(255),
    subject_id VARCHAR(255),
    hadm_id VARCHAR(255),
    note_type VARCHAR(255),
    note_seq VARCHAR(255),
    charttime VARCHAR(255),
    storetime VARCHAR(255),
    text TEXT
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/radiology.csv'
INTO TABLE ml1_project.radiology
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.radiology_detail;
CREATE TABLE ml1_project.radiology_detail (
    note_id VARCHAR(255),
    subject_id VARCHAR(255),
    field_name VARCHAR(255),
    field_value TEXT,
    field_ordinal VARCHAR(255)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/radiology_detail.csv'
INTO TABLE ml1_project.radiology_detail
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


DROP TABLE IF EXISTS ml1_project.services;
CREATE TABLE ml1_project.services (
    subject_id VARCHAR(20),
    hadm_id VARCHAR(20),
    transfertime VARCHAR(50),
    prev_service VARCHAR(50),
    curr_service VARCHAR(50)
);
LOAD DATA LOCAL INFILE 'C:/Users/User/Group_Project/sql_data/services.csv'
INTO TABLE ml1_project.services
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

--admissions
--diagnoses_icd
--discharge
--discharge_detail
--drgcodes
--d_hcpcs
--d_icd_diagnoses
--d_icd_procedures
--d_labitems
--emar
--emar_detail
--hcpcsevents
--labevents
--microbiologyevents
--omr
--patients
--pharmacy
--prescriptions
--procedures_icd
--radiology
--radiology_detail
--services

SELECT 'admissions' AS table_name, COUNT(*) AS row_count FROM admissions UNION ALL
SELECT 'diagnoses_icd', COUNT(*) FROM diagnoses_icd UNION ALL
SELECT 'discharge', COUNT(*) FROM discharge UNION ALL
SELECT 'discharge_detail', COUNT(*) FROM discharge_detail UNION ALL
SELECT 'drgcodes', COUNT(*) FROM drgcodes UNION ALL
SELECT 'd_hcpcs', COUNT(*) FROM d_hcpcs UNION ALL
SELECT 'd_icd_diagnoses', COUNT(*) FROM d_icd_diagnoses UNION ALL
SELECT 'd_icd_procedures', COUNT(*) FROM d_icd_procedures UNION ALL
SELECT 'd_labitems', COUNT(*) FROM d_labitems UNION ALL
SELECT 'emar', COUNT(*) FROM emar UNION ALL
SELECT 'emar_detail', COUNT(*) FROM emar_detail UNION ALL
SELECT 'hcpcsevents', COUNT(*) FROM hcpcsevents UNION ALL
SELECT 'labevents', COUNT(*) FROM labevents UNION ALL
SELECT 'microbiologyevents', COUNT(*) FROM microbiologyevents UNION ALL
SELECT 'omr', COUNT(*) FROM omr UNION ALL
SELECT 'patients', COUNT(*) FROM patients UNION ALL
SELECT 'pharmacy', COUNT(*) FROM pharmacy UNION ALL
SELECT 'prescriptions', COUNT(*) FROM prescriptions UNION ALL
SELECT 'procedures_icd', COUNT(*) FROM procedures_icd UNION ALL
SELECT 'radiology', COUNT(*) FROM radiology UNION ALL
SELECT 'radiology_detail', COUNT(*) FROM radiology_detail UNION ALL
SELECT 'services', COUNT(*) FROM services;

--ROW COUNTS
--admissions            546028
--diagnoses_icd         6364488
--discharge             332072
--discharge_detail      186138
--drgcodes              761856
--d_hcpcs               89208
--d_icd_diagnoses       112107
--d_icd_procedures      86423
--d_labitems            1650
--emar                  42808593
--emar_detail           87371064
--hcpcsevents           186074
--labevents             158374764
--microbiologyevents    3988224
--omr                   7753027
--patients              364627
--pharmacy              17847567
--prescriptions         20292611
--procedures_icd        859655
--radiology             2321355
--radiology_detail      6046121
--services              593071

DELETE FROM admissions WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM diagnoses_icd WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM discharge WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';          --233 rows
DELETE FROM discharge_detail WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM drgcodes WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM emar WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM emar_detail WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM hcpcsevents WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM labevents WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM microbiologyevents WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM omr WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM patients WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM pharmacy WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM prescriptions WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM procedures_icd WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM radiology WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM radiology_detail WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';
DELETE FROM services WHERE subject_id IS NULL OR subject_id = '' OR subject_id = ' ';


--Filtering tables for use

--create a new table for patients who are still alive
DROP TABLE IF EXISTS ml1_project.p01_patients;
CREATE TABLE ml1_project.p01_patients AS
SELECT 
    CAST(subject_id AS CHAR(8)) AS subject_id, 
    CAST(gender AS CHAR(1)) AS gender, 
    CAST(anchor_age AS UNSIGNED) AS anchor_age, 
    CAST(anchor_year AS UNSIGNED) AS anchor_year
FROM ml1_project.patients
WHERE dod = ''
GROUP BY 1,2,3,4;
SELECT * FROM ml1_project.p01_patients LIMIT 10;

--create a new table for discharge summaries
DROP TABLE IF EXISTS ml1_project.p01_discharge;
CREATE TABLE ml1_project.p01_discharge AS
SELECT 
    CAST(d.subject_id AS CHAR(8)) AS subject_id, 
    CAST(d.hadm_id AS CHAR(8)) AS hadm_id, 
    CAST(d.charttime AS DATETIME) AS charttime, 
    d.text
FROM ml1_project.discharge d
INNER JOIN ml1_project.p01_patients p
ON d.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_discharge LIMIT 10;

--create a new table for drg events
DROP TABLE IF EXISTS ml1_project.p01_drgcodes;
create table ml1_project.p01_drgcodes as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    a.description
FROM ml1_project.drgcodes a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3;
SELECT * FROM ml1_project.p01_drgcodes LIMIT 10;

--create a new table for hcpcsevents events
DROP TABLE IF EXISTS ml1_project.p01_hcpcsevents;
create table ml1_project.p01_hcpcsevents as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    CAST(a.chartdate AS DATETIME) AS chartdate, 
    a.short_description
FROM ml1_project.hcpcsevents a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_hcpcsevents LIMIT 10;

--create a new table for emar events
DROP TABLE IF EXISTS ml1_project.p01_emar;
create table ml1_project.p01_emar as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    CAST(a.charttime AS DATETIME) AS charttime, 
    a.medication
FROM ml1_project.emar a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
WHERE event_txt = 'Administered'
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_emar LIMIT 10;

--create a new table for admissions events
DROP TABLE IF EXISTS ml1_project.p01_patients_admissions;
create table ml1_project.p01_patients_admissions as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    a.insurance,
    a.language, 
    a.marital_status, 
    a.race
FROM ml1_project.admissions a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4, 5;
SELECT * FROM ml1_project.p01_patients_admissions LIMIT 10;
SELECT subject_id, COUNT(*) FROM ml1_project.p01_patients_admissions GROUP BY 1 ORDER BY 2 DESC LIMIT 10; 

--create a new table for microbiologyevents events
DROP TABLE IF EXISTS ml1_project.p01_microbiologyevents;
create table ml1_project.p01_microbiologyevents as
SELECT
    CAST(a.subject_id AS CHAR(8)) AS subject_id,
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id,
    CAST(a.chartdate AS DATETIME) AS chartdate, 
    spec_type_desc,
    test_name,
    comments
FROM ml1_project.microbiologyevents a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4, 5, 6;
SELECT * FROM ml1_project.p01_microbiologyevents LIMIT 10;

--create a new table for omr patients events
DROP TABLE IF EXISTS ml1_project.p01_patients_omr;
create table ml1_project.p01_patients_omr as
SELECT 
CAST(subject_id AS CHAR(8)) AS subject_id,
CASE 
    WHEN result_name = 'eGFR' then 0
    WHEN result_name = 'Weight' then 1
    WHEN result_name = 'Weight (Lbs)' then 2
    WHEN result_name = 'Height' then 3
    WHEN result_name = 'Height (Inches)' then 4
    WHEN result_name = 'BMI' then 5
    WHEN result_name = 'BMI (kg/m2)' then 6
    WHEN result_name = 'Blood Pressure' then 7
    WHEN result_name = 'Blood Pressure Sitting' then 8
    WHEN result_name = 'Blood Pressure Lying' then 9
    WHEN result_name = 'Blood Pressure Standing' then 10
    WHEN result_name = 'Blood Pressure Standing (1 min)' then 11
    WHEN result_name = 'Blood Pressure Standing (3 mins)' then 12
END AS result_name,
CAST(chartdate AS DATETIME) AS chartdate,
CAST(seq_num AS UNSIGNED) AS seq_num,
result_value
FROM ml1_project.omr;
SELECT * FROM ml1_project.p01_patients_omr LIMIT 10;

--create a new table for omr patients events
DROP TABLE IF EXISTS ml1_project.p02_patients_omr;
create table ml1_project.p02_patients_omr as
SELECT a.subject_id, a.result_name, a.result_value
FROM ml1_project.p01_patients_omr a
INNER JOIN
(
    SELECT A.subject_id, A.result_name, A.chartdate, B.seq_num
    FROM
    (
        SELECT subject_id, result_name, max(chartdate) as chartdate
        FROM ml1_project.p01_patients_omr
        group by 1,2
    ) a
    INNER JOIN
    (
        SELECT subject_id, result_name, chartdate, max(seq_num) as seq_num
        FROM ml1_project.p01_patients_omr
        group by 1,2,3
    ) b
    ON A.subject_id = B.subject_id and A.result_name = B.result_name and A.chartdate = B.chartdate
) b
on a.subject_id = b.subject_id and a.result_name = b.result_name and a.chartdate = b.chartdate and a.seq_num = b.seq_num
group by 1,2,3;
SELECT * FROM ml1_project.p02_patients_omr LIMIT 10;

--create a new table for omr patients events
DROP TABLE IF EXISTS ml1_project.p03_patients_omr;
create table ml1_project.p03_patients_omr as
SELECT subject_id,
COALESCE(
    MAX(CASE WHEN result_name = 7 THEN result_value END),
    MAX(CASE WHEN result_name = 8 THEN result_value END),
    MAX(CASE WHEN result_name = 9 THEN result_value END),
    MAX(CASE WHEN result_name = 10 THEN result_value END),
    MAX(CASE WHEN result_name = 11 THEN result_value END),
    MAX(CASE WHEN result_name = 12 THEN result_value END)
) AS blood_pressure,
COALESCE(
    MAX(CASE WHEN result_name = 6 THEN result_value END),
    MAX(CASE WHEN result_name = 5 THEN result_value END)
) AS bmi,
COALESCE(
    MAX(CASE WHEN result_name = 4 THEN result_value END),
    MAX(CASE WHEN result_name = 3 THEN result_value END)
) AS height,
COALESCE(
    MAX(CASE WHEN result_name = 2 THEN result_value END),
    MAX(CASE WHEN result_name = 1 THEN result_value END)
) AS weight,
MAX(CASE WHEN result_name = 0 THEN result_value END) AS egfr
FROM ml1_project.p02_patients_omr
GROUP BY subject_id;
SELECT * FROM ml1_project.p03_patients_omr LIMIT 10;

--create a new table for omr patients events
DROP TABLE IF EXISTS ml1_project.p04_patients_omr;
CREATE TABLE ml1_project.p04_patients_omr AS
SELECT 
    a.subject_id, 
    SUBSTRING_INDEX(blood_pressure, '/', 1) AS blood_pressure_systolic,
    SUBSTRING_INDEX(blood_pressure, '/', -1) AS blood_pressure_diastolic,
    CASE 
        WHEN bmi REGEXP '^[0-9]+(\.[0-9]*)?$' THEN CAST(bmi AS DECIMAL(10,2)) 
        ELSE NULL 
    END AS bmi, 
    CASE 
        WHEN height REGEXP '^[0-9]+(\.[0-9]*)?$' THEN CAST(height AS DECIMAL(10,2)) 
        ELSE NULL 
    END AS height, 
    CASE 
        WHEN weight REGEXP '^[0-9]+(\.[0-9]*)?$' THEN CAST(weight AS DECIMAL(10,2)) 
        ELSE NULL 
    END AS weight, 
    CASE 
        WHEN REPLACE(egfr, '>', '') REGEXP '^[0-9]+(\.[0-9]*)?$' 
        THEN CAST(REPLACE(egfr, '>', '') AS DECIMAL(10,2)) 
        ELSE NULL 
    END AS egfr 
FROM ml1_project.p03_patients_omr a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id;
SELECT * FROM ml1_project.p04_patients_omr LIMIT 10;


--create a new table for pharmacy events
drop table if exists ml1_project.p01_pharmacy;
create table ml1_project.p01_pharmacy as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    STR_TO_DATE(NULLIF(starttime, ''), '%Y-%m-%d %H:%i:%s') AS starttime, 
    a.medication
FROM ml1_project.pharmacy a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_pharmacy LIMIT 10;

--create a new table for prescriptions events
drop table if exists ml1_project.p01_prescriptions;
create table ml1_project.p01_prescriptions as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    STR_TO_DATE(NULLIF(starttime, ''), '%Y-%m-%d %H:%i:%s') AS starttime, 
    a.drug
FROM ml1_project.prescriptions a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_prescriptions LIMIT 10;

--create a new table for radiology summaries
DROP TABLE IF EXISTS ml1_project.p01_radiology;
CREATE TABLE ml1_project.p01_radiology AS
SELECT 
    CAST(d.subject_id AS CHAR(8)) AS subject_id, 
    CAST(d.hadm_id AS CHAR(8)) AS hadm_id, 
    CAST(d.charttime AS DATETIME) AS charttime, 
    d.text
FROM ml1_project.radiology d
INNER JOIN ml1_project.p01_patients p
ON d.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_radiology LIMIT 10;

--create a new table for services events
DROP TABLE IF EXISTS ml1_project.p01_services;
create table ml1_project.p01_services as
SELECT 
    CAST(a.subject_id AS CHAR(8)) AS subject_id, 
    CAST(a.hadm_id AS CHAR(8)) AS hadm_id, 
    CAST(a.transfertime AS DATETIME) AS transfertime, 
    a.curr_service
FROM ml1_project.services a
INNER JOIN ml1_project.p01_patients p
ON a.subject_id = p.subject_id
GROUP BY 1, 2, 3, 4;
SELECT * FROM ml1_project.p01_services LIMIT 10;

--getting out features to select relevant ones for aggregation.
SELECT description, count(*), count(DISTINCT subject_id) from ml1_project.p01_drgcodes group by 1 order by 3 desc, 2 desc;
SELECT short_description, count(*), count(DISTINCT subject_id) from ml1_project.p01_hcpcsevents group by 1 order by 3 desc, 2 desc;
SELECT medication, count(*), count(DISTINCT subject_id) from ml1_project.p01_emar group by 1 order by 3 desc, 2 desc;
SELECT medication, count(*), count(DISTINCT subject_id) from ml1_project.p01_pharmacy group by 1 order by 3 desc, 2 desc;
SELECT drug, count(*), count(DISTINCT subject_id) from ml1_project.p01_prescriptions group by 1 order by 3 desc, 2 desc;

SELECT B.long_title, COUNT(*) AS row_count, COUNT(DISTINCT A.subject_id) AS unique_subjects
FROM ml1_project.diagnoses_icd A
JOIN ml1_project.d_icd_diagnoses B
on A.icd_code = B.icd_code and A.icd_version = B.icd_version
GROUP BY 1
order by 3 desc, 2 desc;

SELECT B.long_title, COUNT(*) AS row_count, COUNT(DISTINCT A.subject_id) AS unique_subjects
FROM ml1_project.procedures_icd A
JOIN ml1_project.d_icd_procedures B
on A.icd_code = B.icd_code and A.icd_version = B.icd_version
GROUP BY 1
order by 3 desc, 2 desc;


----------------------------------------------------------------------------------------------------------------
--create a new db

--final patient table
DROP TABLE IF EXISTS ml1_project_clean.patients;
CREATE TABLE ml1_project_clean.patients AS
SELECT a.subject_id, a.gender, a.anchor_age, a.anchor_year,
b.insurance, b.language, b.marital_status, b.race,
c.blood_pressure_systolic, c.blood_pressure_diastolic, c.bmi, c.height, c.weight, c.egfr
FROM ml1_project.p01_patients a
left join (SELECT subject_id, MAX(insurance) as insurance, MAX(language) as language, MAX(marital_status) as marital_status, MAX(race) as race from ml1_project.p01_patients_admissions group by 1) b
on a.subject_id = b.subject_id
left join ml1_project.p04_patients_omr c
on a.subject_id = c.subject_id
WHERE a.subject_id is not null and b.subject_id is not null and c.subject_id is not null and a.subject_id != '' and b.subject_id != '' and c.subject_id != ''
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

DROP TABLE IF EXISTS ml1_project_clean.diagnoses_icd;
CREATE TABLE ml1_project_clean.diagnoses_icd AS
SELECT A.*
FROM ml1_project.diagnoses_icd A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.procedures_icd;
CREATE TABLE ml1_project_clean.procedures_icd AS
SELECT A.*
FROM ml1_project.procedures_icd A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.drgcodes;
CREATE TABLE ml1_project_clean.drgcodes AS
SELECT A.*
FROM ml1_project.p01_drgcodes A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.hcpcsevents;
CREATE TABLE ml1_project_clean.hcpcsevents AS
SELECT A.*
FROM ml1_project.p01_hcpcsevents A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.emar;
CREATE TABLE ml1_project_clean.emar AS
SELECT A.*
FROM ml1_project.p01_emar A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.pharmacy;
CREATE TABLE ml1_project_clean.pharmacy AS
SELECT A.*
FROM ml1_project.p01_pharmacy A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.prescriptions;
CREATE TABLE ml1_project_clean.prescriptions AS
SELECT A.*
FROM ml1_project.p01_prescriptions A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

CREATE TABLE ml1_project_clean.d_icd_diagnoses AS
SELECT A.*
FROM ml1_project.d_icd_diagnoses A;

CREATE TABLE ml1_project_clean.d_icd_procedures AS
SELECT A.*
FROM ml1_project.d_icd_procedures A;

DROP TABLE IF EXISTS ml1_project_clean.discharge;
CREATE TABLE ml1_project_clean.discharge AS
SELECT A.subject_id, A.hadm_id, A.charttime, replace(replace(A.text, '|',''),'"','') as text
FROM ml1_project.p01_discharge A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.radiology;
CREATE TABLE ml1_project_clean.radiology AS
SELECT A.subject_id, A.hadm_id, A.charttime, replace(replace(A.text, '|',''),'"','') as text
FROM ml1_project.p01_radiology A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

DROP TABLE IF EXISTS ml1_project_clean.services;
CREATE TABLE ml1_project_clean.services AS
SELECT A.*
FROM ml1_project.p01_services A
INNER JOIN (SELECT subject_id FROM ml1_project_clean.patients GROUP BY 1) B
ON A.subject_id = B.subject_id;

--admissions            Combined to patients
--diagnoses_icd         Yes
--discharge             Yes
--discharge_detail
--drgcodes              Yes
--d_hcpcs
--d_icd_diagnoses       Yes
--d_icd_procedures      Yes
--d_labitems
--emar                  Yes
--emar_detail
--hcpcsevents           Yes
--labevents
--microbiologyevents
--omr                   Combined to patients
--patients              Yes
--pharmacy              Yes
--prescriptions         Yes
--procedures_icd        Yes
--radiology             Yes
--radiology_detail
--services              Yes


SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/patients.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.patients;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/diagnoses_icd.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.diagnoses_icd;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/discharge.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.discharge;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/drgcodes.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.drgcodes;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/d_icd_diagnoses.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.d_icd_diagnoses;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/d_icd_procedures.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.d_icd_procedures;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/emar.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.emar;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/hcpcsevents.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.hcpcsevents;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/pharmacy.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.pharmacy;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prescriptions.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.prescriptions;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/procedures_icd.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.procedures_icd;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/radiology.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.radiology;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/services.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.services;