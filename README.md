# MachineLearning1_CT

## Data:

Data is stored in _parquet_data.zip_ in Google Drive for Team 2 ML I. Please place the folder in the folder labeled_data_ and unzip the files.

You can also run the shell script named _unzip_datafiles.sh_ to unzip the files for you. Below are the commands to use the shell script

```bash
sh unzip_datafiles.sh
```

Once the files are unzipped, you are ready to use the notebooks in this repository :)


Table descriptions:
- Patients table: patient level information (demographic) ---- patient_cleaned.csv
  - Time focused dataset: blood pressure
  - Cleaned categorical data 
- merged_5000_patient_ratio.csv 
  - Raw format of data from a patient data 

## Data Set Goals:
- Generalized patient patient data (diagnosis, demographic, icd codes, procedures)
  - Extention of the heart of the heart attack data to provide a holistic view of the patient data that could be part of the heart attack clin
- Heart attack focused clinical trial (Main Dataset)
  - Focuses on key columsn for heart attack related data and patients that would be used for the heart attack related clinical trials

## Simple Dataset creation:
- From ALLHAT clinical trial, create a simply list of criteria that would qualify a patient to be in the dataset. This would be like an age requirement. 
  - Filter using dataset from John 
  - No temporal information currently for simplification of problem. **Future Iteration Idea (list in presentation)**
- From dataset, random sample of patients to be categorized as the "gold standard" patients in the data
- Combine dataset of qualifying patients with a few records of unqualified patients to create a dataset with noice in the data

After creation of our patient qualified data with noice, this will be called "generalized_clinical_trial_data.csv"

Data cut of "generalized_clinical_trial_data.csv" will be of heart attack related fields for embeddings. This will be called "heart_attack_clinical_trial_data.csv"

## Tasks To Be Done:
1) Categorization of icd codes to a higher level code icd code 
  - Filter down the dataset to only heart attack related icd codes
2) Map datasets
   - patient
   - 
- Create pipeline of nlp processing for nlp analysis 
- Clean pipeline for reading in data 
    - Radiology & Admissions
- Mapping data:
  - Patient -> 

NLP Experiments:
- Dataset: patient_cleaned.csv
- Data Level: pateint

## Assumptions:
- We are provided a list of gold standard patients that have had a heart attack from our dataset.
  - Random selection from the whole dataset 
- 

NLP Work: 
- NLP on patient data to understand the patient data better by the creation of new columns in the dataset 

Clustering Work:
- Clustering work is to find patients that are most alike to each patient in the dataset. View the clusters to see if there are any patterns in the data.

TODO: 
- John: eda, study criteria, dataset curation, data mapping
- Michael: qualification patient dataset, golden patient selection, and clustering of patients ()
- Sagana: Data pipeline, feature extraction from nlp, data standardization 
- Yeochan: NLP embeddings and clustering work
- Kanav: Data pipeline, feature extraction from nlp, data standardization 


## Dataset Column Names Mapping:
- hadm_ID -> Hospital Admission ID
- subject_id -> Patient ID
- icd9_code -> ICD9 Code