# Import required packages
import sys
import pandas as pd
import numpy as np
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from transformers import AutoTokenizer, AutoModel
import torch
import logging
from typing import List, Dict, Union
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, MapType, ArrayType, FloatType
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Download required NLTK resources
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark Session
def initialize_spark(app_name="Clinical_Notes_Processing", memory="4g"):
    spark = (SparkSession.builder 
             .appName(app_name) 
             .master("local") 
             .config("spark.driver.memory", "8g") 
             .config("spark.executor.memory", "6g")
             .config("spark.driver.maxResultSize", "2g") 
             .config("spark.sql.shuffle.partitions", "100")  
             .config("spark.default.parallelism", "8") 
             .config("spark.local.dir", "/tmp/spark-temp")
             .config("spark.rdd.compress", "true") 
             .config("spark.memory.fraction", "0.8") 
             .config("spark.memory.storageFraction", "0.3") 
             .getOrCreate())
    return spark

def setup_nltk():
    try:
        nltk.download('stopwords')
        nltk.download('wordnet')
        nltk.download('punkt')
        logger.info("NLTK resources downloaded successfully")
    except Exception as e:
        logger.error(f"Error downloading NLTK resources: {e}")
        raise

PATTERNS = {
    'section_headers': r'([A-Z][A-Z\s]{2,}:)',
    'line_breaks': r'\\\n\\\n|\n\n|\r\n\r\n',
    'backslashes': r'\\+',
    'measurements': r'\d+\.?\d*\s*(mm|cm|rad|mrad|cc|ml)',
    'dates': r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{2,4}[-/]\d{1,2}[-/]\d{1,2}',
    'patient_ids': r'(?i)mrn:?\s*\d+|patient\s*id:?\s*\d+|\d{5,10}',
    'special_chars': r'[^a-zA-Z0-9\s.,;:?!()_-]'
}

MAMMOGRAM_ENTITIES = {
    'findings': [
        'mass', 'masses', 'calcification', 'calcifications', 'microcalcification',
        'microcalcifications', 'asymmetry', 'distortion', 'density', 'densities',
        'nodule', 'nodules', 'lesion', 'lesions', 'spiculated'
    ],
    'birads': [
        'bi-rads', 'birads', 'category', 'bi-rads 0', 'bi-rads 1', 'bi-rads 2',
        'bi-rads 3', 'bi-rads 4', 'bi-rads 5', 'bi-rads 6'
    ],
    'locations': [
        'right breast', 'left breast', 'bilateral', 'upper outer', 'upper inner',
        'lower outer', 'lower inner', 'axillary', 'subareolar', 'retroareolar', 
        'quadrant', 'uoq', 'uiq', 'loq', 'liq'
    ],
    'procedures': [
        'screening', 'diagnostic', 'ultrasound', 'biopsy', 'mri', 'tomosynthesis', 
        'cad', 'computer aided detection', '3d', 'spot compression', 'magnification'
    ]
}

STOP_WORDS = set(stopwords.words('english'))
LEMMATIZER = WordNetLemmatizer()

# --- Embedding Setup for Clinical Notes using Bio_ClinicalBERT ---
MODEL_NAME = "emilyalsentzer/Bio_ClinicalBERT"
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
global_tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
global_model = AutoModel.from_pretrained(MODEL_NAME)
global_model.to(DEVICE)
global_model.eval()

def embed_text_func(text: str) -> list:
    """
    Compute embedding for long texts using a chunking approach with batch processing.
    Splits the text into non-overlapping 512-token chunks, processes all chunks
    in a single forward pass, and returns the mean embedding.
    """
    if not text or not isinstance(text, str):
        return []
    # Tokenize the entire text without truncation to capture full length.
    encoding = global_tokenizer(text, return_tensors="pt", truncation=False)
    input_ids = encoding["input_ids"][0]  # shape: (num_tokens,)
    total_tokens = input_ids.size(0)
    max_length = 512
    chunks = []
    for i in range(0, total_tokens, max_length):
        chunks.append(input_ids[i:i+max_length])
    # Batch pad all chunks to max_length
    batch_encoding = global_tokenizer.pad({"input_ids": chunks}, return_tensors="pt", padding="max_length", max_length=max_length)
    batch_encoding = {k: v.to(DEVICE) for k, v in batch_encoding.items()}
    with torch.no_grad():
        outputs = global_model(**batch_encoding)
    token_embeddings = outputs.last_hidden_state  # (batch_size, max_length, hidden_size)
    attention_mask = batch_encoding["attention_mask"]  # (batch_size, max_length)
    mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * mask_expanded, dim=1)
    sum_mask = torch.clamp(mask_expanded.sum(dim=1), min=1e-9)
    chunk_embeddings = sum_embeddings / sum_mask  # (batch_size, hidden_size)
    final_embedding = chunk_embeddings.mean(dim=0)
    return final_embedding.cpu().numpy().tolist()

def clean_text_func(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(PATTERNS['backslashes'], ' ', text)
    text = re.sub(PATTERNS['line_breaks'], ' ', text)
    text = re.sub(PATTERNS['section_headers'], r'\n\1\n', text)
    text = re.sub(PATTERNS['measurements'], r' MEASUREMENT ', text)
    text = re.sub(PATTERNS['dates'], ' DATE ', text)
    text = re.sub(PATTERNS['patient_ids'], ' PATIENT_ID ', text)
    text = re.sub(PATTERNS['special_chars'], ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_sections_func(text: str) -> dict:
    sections = {}
    section_patterns = [
        (r'HISTORY:?(.*?)(?=[A-Z]{2,}:|$)', 'history'),
        (r'FINDINGS:?(.*?)(?=[A-Z]{2,}:|$)', 'findings'),
        (r'IMPRESSION:?(.*?)(?=[A-Z]{2,}:|$)', 'impression'),
        (r'RECOMMENDATION:?(.*?)(?=[A-Z]{2,}:|$)', 'recommendation'),
        (r'COMPARISON:?(.*?)(?=[A-Z]{2,}:|$)', 'comparison'),
        (r'TECHNIQUE:?(.*?)(?=[A-Z]{2,}:|$)', 'technique'),
        (r'BIRADS:?(.*?)(?=[A-Z]{2,}:|$)', 'birads'),
        (r'INDICATION:?(.*?)(?=[A-Z]{2,}:|$)', 'indication'),
        (r'EXAMINATION:?(.*?)(?=[A-Z]{2,}:|$)', 'examination'),
        (r'CLINICAL HISTORY:?(.*?)(?=[A-Z]{2,}:|$)', 'clinical_history'),
        (r'BILATERAL DIGITAL SCREENING MAMMOGRAM WITH CAD HISTORY:?(.*?)(?=[A-Z]{2,}:|$)',
         'bilateral_digital_screening_mammogram_with_cad_history'),
    ]
    for pattern, section_name in section_patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            sections[section_name] = match.group(1).strip()
    if not sections:
        sections['full_text'] = text
    return sections

def extract_entities_func(text: str) -> dict:
    if not text or not isinstance(text, str):
        return {k: [] for k in MAMMOGRAM_ENTITIES.keys()}
    text_lower = text.lower()
    entities = {k: [] for k in MAMMOGRAM_ENTITIES.keys()}
    birads_match = re.search(r'bi-?rads\s*(?:category)?\s*([0-6])[^0-6]', text_lower, re.IGNORECASE)
    if birads_match:
        entities['birads'].append(f"BI-RADS {birads_match.group(1)}")
    for category, terms in MAMMOGRAM_ENTITIES.items():
        for term in terms:
            if term in text_lower:
                entities[category].append(term)
    for category in entities:
        entities[category] = list(set(entities[category]))
    return entities

def preprocess_text_func(text: str) -> str:
    text = text.lower()
    tokens = nltk.word_tokenize(text)
    filtered_tokens = [token for token in tokens if token not in STOP_WORDS and len(token) > 2]
    lemmatized_tokens = [LEMMATIZER.lemmatize(token) for token in filtered_tokens]
    return ' '.join(lemmatized_tokens)

class MammogramNotesProcessor:
    def __init__(self, use_spark=False, spark=None):
        self.use_spark = use_spark
        self.spark = spark if use_spark else None
        self.stop_words = STOP_WORDS
        self.lemmatizer = LEMMATIZER
        self.patterns = PATTERNS
        self.mammogram_entities = MAMMOGRAM_ENTITIES
        if self.use_spark and self.spark:
            self._register_spark_udfs()
    
    def _register_spark_udfs(self):
        self.clean_text_udf = udf(clean_text_func, StringType())
        self.extract_sections_udf = udf(extract_sections_func, MapType(StringType(), StringType()))
        self.extract_entities_udf = udf(extract_entities_func, MapType(StringType(), ArrayType(StringType())))
        self.preprocess_text_udf = udf(preprocess_text_func, StringType())
        self.embed_text_udf = udf(embed_text_func, ArrayType(FloatType()))
    
    def clean_text(self, text: str) -> str:
        return clean_text_func(text)
    
    def extract_sections(self, text: str) -> Dict[str, str]:
        return extract_sections_func(text)
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        return extract_entities_func(text)
    
    def preprocess_text(self, text: str) -> str:
        return preprocess_text_func(text)
    
    def process_note(self, note: str) -> Dict:
        result = {}
        result['raw_text'] = note
        result['cleaned_text'] = self.clean_text(note)
        result['sections'] = self.extract_sections(result['cleaned_text'])
        result['entities'] = self.extract_entities(result['cleaned_text'])
        result['preprocessed_text'] = self.preprocess_text(result['cleaned_text'])
        result['embedding'] = embed_text_func(result['preprocessed_text'])
        result['birads_category'] = None
        if 'birads' in result['entities'] and result['entities']['birads']:
            for item in result['entities']['birads']:
                if 'bi-rads' in item.lower():
                    result['birads_category'] = item
        return result
    
    def process_notes_pandas(self, notes_df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        logger.info(f"Processing {len(notes_df)} mammogram notes with pandas")
        processed_notes = []
        for i, row in notes_df.iterrows():
            note_text = row[text_column]
            processed = self.process_note(note_text)
            for col in notes_df.columns:
                if col != text_column:
                    processed[col] = row[col]
            processed_notes.append(processed)
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1} notes")
        return pd.DataFrame(processed_notes)
    
    def process_notes_spark(self, notes_df, text_column: str):
        if not self.use_spark or not self.spark:
            raise ValueError("Spark session is required for Spark processing")
        logger.info("Processing mammogram notes with Spark")
        notes_df = notes_df.withColumn("cleaned_text", self.clean_text_udf(col(text_column)))
        notes_df = notes_df.withColumn("sections", self.extract_sections_udf(col("cleaned_text")))
        notes_df = notes_df.withColumn("entities", self.extract_entities_udf(col("cleaned_text")))
        notes_df = notes_df.withColumn("preprocessed_text", self.preprocess_text_udf(col("cleaned_text")))
        notes_df = notes_df.withColumn("embedding", self.embed_text_udf(col("preprocessed_text")))
        return notes_df

def process_mammogram_notes(input_file, text_column, output_file, use_spark=False):
    logger.info("Starting mammogram notes processing")
    setup_nltk()
    
    if use_spark:
        spark = initialize_spark()
        processor = MammogramNotesProcessor(use_spark=True, spark=spark)
        notes_df = spark.read.parquet(input_file)
        processed_df = processor.process_notes_spark(notes_df, text_column)
        processed_df.write.mode("overwrite").parquet(output_file)
        spark.stop()
    else:
        processor = MammogramNotesProcessor(use_spark=False)
        notes_df = pd.read_csv(input_file)
        processed_df = processor.process_notes_pandas(notes_df, text_column)
        processed_df.to_csv(output_file, index=False)
    
    logger.info(f"Mammogram notes processing complete. Results saved to {output_file}")

if __name__ == "__main__":
    args = sys.argv[1:]
    input_file_path = args[0]
    output_file_path = args[1]
    
    logger.info(f"Input file path: {input_file_path}")
    logger.info(f"Output file path: {output_file_path}")
    
    process_mammogram_notes(input_file_path, "text", output_file_path, use_spark=True)
    logger.info("SUCCESS: Processing complete")