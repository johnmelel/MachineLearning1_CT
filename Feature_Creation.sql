select * FROM ml1_project_clean.procedures_icd limit 10;

select long_title, count(*) as dcdc FROM ml1_project_clean.d_icd_procedures order by 2 desc limit 10;

SELECT long_title, category FROM ml1_project_clean.d_icd_procedures_mapping;
SELECT short_description, category FROM ml1_project_clean.hcpcs_mapping;
SELECT description, category FROM ml1_project_clean.drg_codes_mapping;
SELECT medication, category FROM ml1_project_clean.emar_mapping;
SELECT long_title, category FROM ml1_project_clean.icd_diagnoses_mapping;
SELECT medication, category FROM ml1_project_clean.pharmacy_mapping;
SELECT drug, category FROM ml1_project_clean.prescriptions_mapping;

DROP TABLE IF EXISTS ml1_project_clean.p01_d_icd_procedures;
CREATE TABLE ml1_project_clean.p01_d_icd_procedures as
SELECT A.icd_code, A.icd_version, A.long_title, B.category
FROM ml1_project_clean.d_icd_procedures A
LEFT JOIN ml1_project_clean.d_icd_procedures_mapping B
ON A.long_title = B.long_title;
SELECT * FROM ml1_project_clean.p01_d_icd_procedures limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_icd_procedures_features;
CREATE TABLE ml1_project_clean.p01_icd_procedures_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cardiovascular Procedures' THEN num_dates ELSE 0 END) AS cardiovascular_px,
MAX(CASE WHEN category = 'Renal Procedures' THEN num_dates ELSE 0 END) AS renal_px
FROM
(
    SELECT A.subject_id, B.category, COUNT(DISTINCT chartdate) as num_dates
    FROM ml1_project_clean.procedures_icd A
    INNER JOIN ml1_project_clean.p01_d_icd_procedures B
    ON A.icd_code = B.icd_code AND A.icd_version = B.icd_version
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_icd_procedures_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_hcpcsevents_features;
CREATE TABLE ml1_project_clean.p01_hcpcsevents_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cardiovascular' THEN num_dates ELSE 0 END) AS cardiovascular_hcpcs_px
FROM
(
    SELECT A.subject_id, B.category, COUNT(DISTINCT chartdate) as num_dates
    FROM ml1_project_clean.hcpcsevents A
    INNER JOIN ml1_project_clean.hcpcs_mapping B
    ON A.short_description = B.short_description
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_hcpcsevents_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_drgcodes_features;
CREATE TABLE ml1_project_clean.p01_drgcodes_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cardiovascular' THEN num_dates ELSE 0 END) AS cardiovascular_drg_dx,
MAX(CASE WHEN category = 'Kidney Disease' THEN num_dates ELSE 0 END) AS kidney_drg_dx,
MAX(CASE WHEN category = 'Diabetes' THEN num_dates ELSE 0 END) AS diabetes_drg_dx,
MAX(CASE WHEN category = 'Hypertension' THEN num_dates ELSE 0 END) AS hypertension_drg_dx
FROM
(
    SELECT A.subject_id, B.category, COUNT(DISTINCT hadm_id) as num_dates
    FROM ml1_project_clean.drgcodes A
    INNER JOIN ml1_project_clean.drg_codes_mapping B
    ON A.description = B.description
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_drgcodes_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_d_icd_diagnoses;
CREATE TABLE ml1_project_clean.p01_d_icd_diagnoses as
SELECT A.icd_code, A.icd_version, A.long_title, B.category
FROM ml1_project_clean.d_icd_diagnoses A
LEFT JOIN ml1_project_clean.icd_diagnoses_mapping B
ON A.long_title = B.long_title;
SELECT * FROM ml1_project_clean.p01_d_icd_diagnoses limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_icd_diagnoses_features;
CREATE TABLE ml1_project_clean.p01_icd_diagnoses_features as
SELECT subject_id, MAX(CASE WHEN category = 'Heart Disease' THEN num_dates ELSE 0 END) AS heart_disease_dx,
MAX(CASE WHEN category = 'Kidney Disease' THEN num_dates ELSE 0 END) AS kidney_disease_dx,
MAX(CASE WHEN category = 'Diabetes' THEN num_dates ELSE 0 END) AS diabetes_dx,
MAX(CASE WHEN category = 'Cholesterol' THEN num_dates ELSE 0 END) AS cholesterol_dx,
MAX(CASE WHEN category = 'Nicotine Dependence' THEN num_dates ELSE 0 END) AS nicotine_dependence_dx,
MAX(CASE WHEN category = 'Hypertension' THEN num_dates ELSE 0 END) AS hypertension_dx
FROM
(
    SELECT A.subject_id, B.category, COUNT(DISTINCT concat(hadm_id,seq_num)) as num_dates
    FROM ml1_project_clean.diagnoses_icd A
    INNER JOIN ml1_project_clean.p01_d_icd_diagnoses B
    ON A.icd_code = B.icd_code AND A.icd_version = B.icd_version
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_icd_diagnoses_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_emar_features;
CREATE TABLE ml1_project_clean.p01_emar_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cholesterol' THEN num_dates ELSE 0 END) AS cholesterol_emar_rx,
MAX(CASE WHEN category = 'Kidney' THEN num_dates ELSE 0 END) AS kidney_emar_rx,
MAX(CASE WHEN category = 'Heart' THEN num_dates ELSE 0 END) AS heart_emar_rx,
MAX(CASE WHEN category = 'Hypertension' THEN num_dates ELSE 0 END) AS hypertension_emar_rx,
MAX(CASE WHEN category = 'Diabetes' THEN num_dates ELSE 0 END) AS diabetes_emar_rx
FROM
(
    SELECT A.subject_id, B.category, COUNT(DISTINCT charttime) as num_dates
    FROM ml1_project_clean.emar A
    INNER JOIN ml1_project_clean.emar_mapping B
    ON A.medication = B.medication
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_emar_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_pharmacy_features;
CREATE TABLE ml1_project_clean.p01_pharmacy_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cholesterol' THEN num_dates ELSE 0 END) AS cholesterol_pharmacy_rx,
MAX(CASE WHEN category = 'Kidney' THEN num_dates ELSE 0 END) AS kidney_pharmacy_rx,
MAX(CASE WHEN category = 'Heart' THEN num_dates ELSE 0 END) AS heart_pharmacy_rx,
MAX(CASE WHEN category = 'Hypertension' THEN num_dates ELSE 0 END) AS hypertension_pharmacy_rx,
MAX(CASE WHEN category = 'Diabetes' THEN num_dates ELSE 0 END) AS diabetes_pharmacy_rx
FROM
(
    SELECT A.subject_id, B.category, COUNT(*) as num_dates
    FROM ml1_project_clean.pharmacy A
    INNER JOIN ml1_project_clean.pharmacy_mapping B
    ON A.medication = B.medication
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_pharmacy_features limit 10;

DROP TABLE IF EXISTS ml1_project_clean.p01_prescription_features;
CREATE TABLE ml1_project_clean.p01_prescription_features as
SELECT subject_id, MAX(CASE WHEN category = 'Cholesterol' THEN num_dates ELSE 0 END) AS cholesterol_prescription_rx,
MAX(CASE WHEN category = 'Kidney' THEN num_dates ELSE 0 END) AS kidney_prescription_rx,
MAX(CASE WHEN category = 'Heart' THEN num_dates ELSE 0 END) AS heart_prescription_rx,
MAX(CASE WHEN category = 'Hypertension' THEN num_dates ELSE 0 END) AS hypertension_prescription_rx,
MAX(CASE WHEN category = 'Diabetes' THEN num_dates ELSE 0 END) AS diabetes_prescription_rx
FROM
(
    SELECT A.subject_id, B.category, COUNT(*) as num_dates
    FROM ml1_project_clean.prescriptions A
    INNER JOIN ml1_project_clean.prescriptions_mapping B
    ON A.drug = B.drug
    WHERE B.category IS NOT NULL
    GROUP BY 1,2
) A
GROUP BY 1;
SELECT * FROM ml1_project_clean.p01_prescription_features limit 10;


DROP TABLE IF EXISTS ml1_project_clean.patient_features;
CREATE TABLE ml1_project_clean.patient_features as
SELECT A.*,
B.cardiovascular_px, B.renal_px,
C.cardiovascular_hcpcs_px,
D.cardiovascular_drg_dx, D.kidney_drg_dx, D.diabetes_drg_dx, D.hypertension_drg_dx,
E.heart_disease_dx, E.kidney_disease_dx, E.diabetes_dx, E.cholesterol_dx, E.nicotine_dependence_dx, E.hypertension_dx,
F.cholesterol_emar_rx, F.kidney_emar_rx, F.heart_emar_rx, F.hypertension_emar_rx, F.diabetes_emar_rx,
G.cholesterol_pharmacy_rx, G.kidney_pharmacy_rx, G.heart_pharmacy_rx, G.hypertension_pharmacy_rx, G.diabetes_pharmacy_rx,
H.cholesterol_prescription_rx, H.kidney_prescription_rx, H.heart_prescription_rx, H.hypertension_prescription_rx, H.diabetes_prescription_rx
FROM ml1_project_clean.patients as A
LEFT JOIN ml1_project_clean.p01_icd_procedures_features as B
ON A.subject_id = B.subject_id
LEFT JOIN ml1_project_clean.p01_hcpcsevents_features as C
ON A.subject_id = C.subject_id
LEFT JOIN ml1_project_clean.p01_drgcodes_features as D
ON A.subject_id = D.subject_id
LEFT JOIN ml1_project_clean.p01_icd_diagnoses_features as E
ON A.subject_id = E.subject_id
LEFT JOIN ml1_project_clean.p01_emar_features as F
ON A.subject_id = F.subject_id
LEFT JOIN ml1_project_clean.p01_pharmacy_features as G
ON A.subject_id = G.subject_id
LEFT JOIN ml1_project_clean.p01_prescription_features as H
ON A.subject_id = H.subject_id;
SELECT * FROM ml1_project_clean.patient_features limit 10;

SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/patient_features.csv'
FIELDS TERMINATED BY '|' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM ml1_project_clean.patient_features;
'subject_id', 'gender', 'anchor_age', 'anchor_year', 'insurance', 'language', 'marital_status', 'race', 'blood_pressure_systolic', 'blood_pressure_diastolic', 'bmi', 'height', 'weight', 'egfr cardiovascular_px', 'renal_px', 'cardiovascular_hcpcs_px', 'cardiovascular_drg_dx', 'kidney_drg_dx', 'diabetes_drg_dx', 'hypertension_drg_dx', 'heart_disease_dx', 'kidney_disease_dx', 'diabetes_dx', 'cholesterol_dx', 'nicotine_dependence_dx', 'hypertension_dx', 'cholesterol_emar_rx', 'kidney_emar_rx', 'heart_emar_rx', 'hypertension_emar_rx', 'diabetes_emar_rx', 'cholesterol_pharmacy_rx', 'kidney_pharmacy_rx', 'heart_pharmacy_rx', 'hypertension_pharmacy_rx', 'diabetes_pharmacy_rx', 'cholesterol_prescription_rx', 'kidney_prescription_rx', 'heart_prescription_rx', 'hypertension_prescription_rx', 'diabetes_prescription_rx'

