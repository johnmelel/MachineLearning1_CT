#!/usr/bin/env python
"""
Script for scalable processing of clinical notes with Spark.
It performs:
  - Lightweight cleaning and normalization of the text.
  - Extraction of sentences that contain key (gold‐standard) entities.
  - Embedding computation using a ClinicalBERT model with chunking.
  - Distributed processing via Spark Pandas UDFs.
  
This approach minimizes expensive summarization/spaCy calls, focuses on mention-level embeddings,
and is optimized for large datasets.
"""

import sys
import os
import re
import logging
import numpy as np
import pandas as pd
import nltk
import torch
from transformers import AutoTokenizer, AutoModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import col
from pyspark.sql.functions import pandas_udf, PandasUDFType

import ssl

# Setup SSL context if needed
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
    
# Ensure necessary NLTK data is downloaded.
nltk.download('punkt', quiet=True)

# ----------------------
# Logging Configuration
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------------
# Device Selection
# ----------------------
DEVICE = torch.device("cpu")
logger.info(f"Using device: {DEVICE}")

# ----------------------
# Global Model Setup
# ----------------------
MODEL_NAME = "emilyalsentzer/Bio_ClinicalBERT"
# Global variables for lazy initialization on each executor.
_GLOBAL_MODEL = None
_GLOBAL_TOKENIZER = None

def get_model_and_tokenizer():
    """
    Lazy-load the Hugging Face model and tokenizer.
    Each Spark executor calls this only once.
    """
    global _GLOBAL_MODEL, _GLOBAL_TOKENIZER
    if _GLOBAL_MODEL is None or _GLOBAL_TOKENIZER is None:
        _GLOBAL_TOKENIZER = AutoTokenizer.from_pretrained(MODEL_NAME)
        _GLOBAL_MODEL = AutoModel.from_pretrained(MODEL_NAME)
        _GLOBAL_MODEL.to(DEVICE)
        _GLOBAL_MODEL.eval()
        logger.info("Model and tokenizer loaded on executor.")
    return _GLOBAL_MODEL, _GLOBAL_TOKENIZER

# ----------------------
# Gold-Standard Keywords
# ----------------------
GOLD_KEYWORDS = set([
    'mass', 'masses', 'calcification', 'calcifications', 'microcalcification', 'microcalcifications',
    'asymmetry', 'distortion', 'density', 'densities', 'nodule', 'nodules', 'lesion', 'lesions',
    'spiculated', 'bi-rads', 'birads', 'category', 'screening', 'diagnostic', 'ultrasound',
    'biopsy', 'mri', 'tomosynthesis', 'cad', 'computer aided detection', '3d',
    'spot compression', 'magnification', 'right breast', 'left breast', 'bilateral', 
    'upper outer', 'upper inner', 'lower outer', 'lower inner', 'axillary', 
    'subareolar', 'retroareolar', 'quadrant'
])

# ----------------------
# Text Preprocessing Functions
# ----------------------
def clean_text(text: str) -> str:
    """
    Basic cleaning: remove extra whitespace, unwanted backslashes,
    and ensure the text is lowercased.
    """
    if not text or not isinstance(text, str):
        return ""
    # Remove backslashes and multiple spaces
    text = re.sub(r'\\+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()

def extract_entity_sentences(text: str) -> list:
    """
    Splits the text into sentences and returns those that contain at least one
    gold-standard keyword. If none found, returns an empty list.
    """
    sentences = nltk.sent_tokenize(text)
    entity_sentences = []
    for sentence in sentences:
        lower_sentence = sentence.lower()
        if any(keyword in lower_sentence for keyword in GOLD_KEYWORDS):
            entity_sentences.append(sentence)
    return entity_sentences

# ----------------------
# Embedding Function
# ----------------------
def embed_text(text: str) -> list:
    """
    Computes the embedding for the given text.
    Uses mention-level extraction: if entity-specific sentences are found,
    only those sentences are embedded. Otherwise, the full text is processed.
    For long texts, the function chunks tokenized inputs into pieces (max 512 tokens),
    computes mean-pooled embeddings for each chunk, and then averages them.
    """
    if not text or not isinstance(text, str):
        return []
    
    # Clean and extract relevant sentences.
    cleaned = clean_text(text)
    entity_sentences = extract_entity_sentences(cleaned)
    # If entity sentences exist, focus on them; else, use full text.
    texts_to_embed = entity_sentences if entity_sentences else [cleaned]
    
    model, tokenizer = get_model_and_tokenizer()
    all_chunk_embeddings = []

    for t in texts_to_embed:
        # Tokenize without truncation to get full token list.
        encoding = tokenizer(t, return_tensors="pt", truncation=False)
        input_ids = encoding["input_ids"][0]
        total_tokens = input_ids.size(0)
        max_length = 512
        chunk_embeddings = []
        # Process in non-overlapping chunks.
        for i in range(0, total_tokens, max_length):
            chunk_ids = input_ids[i:i+max_length].unsqueeze(0)  # shape: [1, chunk_len]
            # Create a basic attention mask (all ones).
            attention_mask = torch.ones_like(chunk_ids)
            with torch.no_grad():
                outputs = model(input_ids=chunk_ids.to(DEVICE),
                                attention_mask=attention_mask.to(DEVICE))
            # Mean pooling over the tokens.
            token_embeddings = outputs.last_hidden_state  # [1, seq_len, hidden_dim]
            chunk_embedding = token_embeddings.mean(dim=1)  # [1, hidden_dim]
            chunk_embeddings.append(chunk_embedding)
        if chunk_embeddings:
            # Average across chunks for this text.
            doc_embedding = torch.cat(chunk_embeddings, dim=0).mean(dim=0)
            all_chunk_embeddings.append(doc_embedding)
    
    if all_chunk_embeddings:
        # If multiple sentences (mentions) are embedded, average them.
        final_embedding = torch.stack(all_chunk_embeddings, dim=0).mean(dim=0)
    else:
        # Fallback: a zero vector.
        final_embedding = torch.zeros(model.config.hidden_size)
    
    return final_embedding.cpu().numpy().tolist()

# ----------------------
# Spark Pandas UDFs
# ----------------------
@pandas_udf(ArrayType(FloatType()))
def embed_text_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF to compute embeddings for a series of texts.
    This function applies the embed_text() function to each element.
    """
    return texts.apply(embed_text)

@pandas_udf(StringType())
def clean_text_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF to clean text.
    """
    return texts.apply(clean_text)

# ----------------------
# Spark Processing Pipeline
# ----------------------
def process_notes_spark(input_path: str, output_path: str) -> None:
    """
    Processes clinical notes from a Spark-readable parquet file, computes note-level embeddings for each text column,
    aggregates them per patient (subject_id) via element-wise averaging, and writes the patient-level embeddings to a parquet file.
    The output file has columns 'subject_id', 'embedded_radiology_text', and 'embedded_discharge_text'.
    """
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("Clinical_Notes_Patient_Level_Embeddings")
             .config("spark.driver.memory", "8g")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.maxResultSize", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.executor.cores", "4")
             .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized.")
    
    # Define Aggregator UDF inside Spark context to ensure an active Spark session
    @pandas_udf('array<float>', PandasUDFType.GROUPED_AGG)
    def avg_embedding_udf(v: pd.Series) -> list:
        import numpy as np
        if v.empty:
            return []
        arr = np.array(v.tolist())
        return np.mean(arr, axis=0).tolist()

    # Read input dataset with only needed columns
    notes_df = spark.read.parquet(input_path).select('subject_id', 'radiology_text', 'discharge_text')
    logger.info(f"Input data read from {input_path}")

    text_columns = ['radiology_text', 'discharge_text']

    # For each text column, apply cleaning and compute note-level embeddings
    for text_column in text_columns:
        cleaned_col = f"cleaned_{text_column}_text"
        embedding_col = f"embedding_{text_column}"
        notes_df = notes_df.withColumn(cleaned_col, clean_text_udf(col(text_column)))
        notes_df = notes_df.withColumn(embedding_col, embed_text_udf(col(cleaned_col)))
        logger.info(f"Processed {text_column} for note-level embeddings.")

    # Group by subject_id and aggregate embeddings using element-wise averaging
    # It is assumed that each subject_id may have multiple notes; we average the embeddings to produce one patient-level embedding per column.
    aggregated_df = notes_df.groupBy('subject_id').agg(
        avg_embedding_udf(col('embedding_radiology_text')).alias('embedded_radiology_text'),
        avg_embedding_udf(col('embedding_discharge_text')).alias('embedded_discharge_text')
    )

    logger.info("Aggregation to patient-level embeddings complete.")

    # Write the result as a parquet file with snappy compression
    aggregated_df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    logger.info(f"Results written to {output_path}")
    spark.stop()

# ----------------------
# Main Function
# ----------------------
def main():
    if len(sys.argv) < 3:
        logger.error("Usage: script.py <input_path> <output_path>")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    process_notes_spark(input_path, output_path)
    logger.info("SUCCESS: Patient-level embedding processing complete.")

if __name__ == "__main__":
    main()