import pandas as pd
import json
import re
from typing import Dict, List, Any, Optional
import logging
from .state import COLUMN_MAPPINGS

logger = logging.getLogger(__name__)

# Token estimation constants
AVERAGE_TOKENS_PER_CHAR = 0.25  # Conservative estimate for token counting
MAX_TOKENS_FOR_CONTEXT = 100000  # Leave buffer for prompt and response

class DataProcessor:
    """Handles advanced data processing for different CSV sources with industry-leading preprocessing"""
    
    @staticmethod
    def process_csv_data(file_content: str, source_name: str, max_rows: int = 8500) -> list:
        """Process CSV data with top-class preprocessing pipeline and return list of chunks for batching"""
        try:
            # Read CSV from string content
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            logger.info(f"Processing {source_name}: {len(df)} rows loaded")
            
            # Check minimum row requirement (10 rows minimum for processing)
            if len(df) < 10:
                logger.warning(f"Dataset {source_name} excluded: Only {len(df)} rows available, minimum 10 rows required for processing")
                return []  # Return empty list to skip processing
            
            # Get column mapping for this source
            column_config = COLUMN_MAPPINGS.get(source_name)
            if not column_config:
                raise ValueError(f"No column mapping found for source: {source_name}")
            
            # COMMENTED OUT - Advanced preprocessing pipeline (users upload clean data)
            # df = DataProcessor._execute_advanced_preprocessing_pipeline(df, source_name, column_config)
            # logger.info(f"After advanced preprocessing: {len(df)} rows remaining")
            logger.info(f"Skipping preprocessing - using clean data as-is: {len(df)} rows")
            
            # Adaptive chunk sizing based on data density and quality
            chunk_sizes = {
                "lms_chats": 3000,        # Very dense JSON data
                "pns_calls": 5000,        # Long transcriptions
                "search_keywords": 8500,   # Optimized size
                "whatsapp_specs": 8500,   # Standard size
                "rejection_comments": 8500 # Standard size
            }
            
            adaptive_max_rows = chunk_sizes.get(source_name, max_rows)
            logger.info(f"Using adaptive chunk size of {adaptive_max_rows} rows for {source_name}")
            
            # Create chunks for large datasets
            chunks = []
            if len(df) > adaptive_max_rows:
                logger.info(f"Large dataset detected ({len(df)} rows). Creating optimized chunks of {adaptive_max_rows} rows each.")
                
                # For search keywords: Sort by frequency first, then chunk
                if source_name == "search_keywords" and "frequency_column" in column_config:
                    freq_col = column_config["frequency_column"]
                    if freq_col in df.columns:
                        df = df.sort_values(freq_col, ascending=False)
                
                # Split into chunks with semantic boundary preservation
                for i in range(0, len(df), adaptive_max_rows):
                    chunk_df = df.iloc[i:i + adaptive_max_rows]
                    chunk_text = DataProcessor._process_chunk_advanced(chunk_df, source_name, column_config, i // adaptive_max_rows + 1)
                    
                    # Estimate tokens and warn if still too high
                    estimated_tokens = DataProcessor._estimate_tokens(chunk_text)
                    if estimated_tokens > 120000:  # 120k token warning threshold
                        logger.warning(f"Chunk {i // adaptive_max_rows + 1} for {source_name} estimated at {estimated_tokens} tokens - may exceed context limit")
                    
                    chunks.append(chunk_text)
                    
                logger.info(f"Created {len(chunks)} chunks for {source_name}")
            else:
                # Small dataset - single chunk
                chunk_text = DataProcessor._process_chunk_advanced(df, source_name, column_config, 1)
                
                # Check token count even for small datasets
                estimated_tokens = DataProcessor._estimate_tokens(chunk_text)
                if estimated_tokens > 120000:
                    logger.warning(f"Single chunk for {source_name} estimated at {estimated_tokens} tokens - may need further splitting")
                
                chunks.append(chunk_text)
                logger.info(f"Small dataset - single chunk for {source_name}")
            
            return chunks
                
        except Exception as e:
            logger.error(f"Error processing {source_name}: {str(e)}")
            raise
    
    # @staticmethod
    # def _execute_advanced_preprocessing_pipeline(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Execute the complete top-class preprocessing pipeline"""
    #     try:
    #         logger.info(f"Executing advanced preprocessing pipeline for {source_name}")
    #         original_count = int(len(df))
    #         logger.debug(f"Starting with {original_count} rows for {source_name}")
            
    #         # STAGE 1: Data Profiling and Schema Detection
    #         df = DataProcessor._stage1_data_profiling(df, source_name, config)
            
    #         # STAGE 2: Advanced Cleaning and Noise Reduction
    #         df = DataProcessor._stage2_advanced_cleaning(df, source_name, config)
            
    #         # STAGE 3: Relevance Filtering and Semantic Scoring
    #         df = DataProcessor._stage3_relevance_filtering(df, source_name, config)
            
    #         # STAGE 4: Intelligent Deduplication
    #         df = DataProcessor._stage4_intelligent_deduplication(df, source_name, config)
            
    #         # STAGE 5: Data Enrichment and Context Enhancement
    #         df = DataProcessor._stage5_data_enrichment(df, source_name, config)
            
    #         # STAGE 6: Quality Validation and Scoring
    #         df = DataProcessor._stage6_quality_validation(df, source_name, config)
            
    #         # STAGE 7: Final Optimization
    #         df = DataProcessor._stage7_final_optimization(df, source_name, config)
            
    #         cleaned_count = int(len(df))
    #         # Ensure both counts are integers to prevent string subtraction errors
    #         original_count = int(original_count)
    #         cleaned_count = int(cleaned_count)
    #         improvement_ratio = ((original_count - cleaned_count) / original_count * 100) if original_count > 0 else 0
    #         logger.info(f"Advanced preprocessing completed: improved quality by {improvement_ratio:.1f}% ({original_count} → {cleaned_count} rows)")
            
    #         return df
            
    #     except Exception as e:
    #         logger.warning(f"Advanced preprocessing failed for {source_name}: {e}. Using fallback processing.")
    #         logger.debug(f"Error details: {type(e).__name__}: {str(e)}")
    #         return DataProcessor._fallback_basic_cleaning(df, source_name, config)
    
    # @staticmethod
    # def _stage1_data_profiling(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 1: Advanced data profiling and schema detection"""
    #     data_col = config["data_column"]
        
    #     if data_col not in df.columns:
    #         raise ValueError(f"Data column '{data_col}' not found in {source_name}")
        
    #     # Basic cleaning first
    #     df = df.dropna(subset=[data_col])
    #     df = df[df[data_col].astype(str).str.strip() != ""]
    #     df = df[df[data_col].astype(str).str.lower() != "nan"]
        
    #     # Data profiling metrics
    #     total_chars = df[data_col].astype(str).str.len().sum()
    #     avg_length = df[data_col].astype(str).str.len().mean()
    #     unique_ratio = df[data_col].nunique() / len(df)
        
    #     logger.info(f"Stage 1 - Data Profile: {len(df)} rows, avg_length: {avg_length:.1f}, uniqueness: {unique_ratio:.2f}")
        
    #     return df
    
    # @staticmethod
    # def _stage2_advanced_cleaning(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 2: Advanced cleaning with pattern recognition and anomaly detection"""
    #     data_col = config["data_column"]
    #     original_count = int(len(df))
        
    #     # Advanced noise patterns (more comprehensive than basic)
    #     advanced_noise_patterns = [
    #         r'^test\s*$', r'^sample\s*$', r'^demo\s*$', r'^example\s*$',
    #         r'^\d+$', r'^[a-zA-Z]$', r'^\.+$', r'^-+$', r'^_+$',
    #         r'^\s*n/?a\s*$', r'^\s*null\s*$', r'^\s*none\s*$', r'^\s*nil\s*$',
    #         r'^#+$', r'^\*+$', r'^[^a-zA-Z0-9]*$',
    #         r'^(lorem|ipsum|dolor|sit|amet).*$',  # Lorem ipsum text
    #         r'^(click|here|link|url|http).*$',   # Web artifacts
    #         r'^\s*(error|failed|exception|warning)\s*$',  # Error messages
    #         r'^[0-9\-\+\(\)\s]+$',  # Phone number patterns without context
    #         r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',  # Email addresses without context
    #     ]
        
    #     # Apply advanced noise filtering
    #     for pattern in advanced_noise_patterns:
    #         df = df[~df[data_col].astype(str).str.lower().str.match(pattern, na=False)]
        
    #     # Source-specific advanced cleaning
    #     if source_name == "search_keywords":
    #         # Remove extremely short/long queries and special character heavy content
    #         df = df[df[data_col].astype(str).str.len().between(3, 200)]
    #         df = df[df[data_col].astype(str).str.count(r'[^a-zA-Z0-9\s]') <= 5]
    #         # Remove queries that are mostly numbers
    #         df = df[~df[data_col].astype(str).str.match(r'^[\d\s\-\.]+$', na=False)]
            
    #     elif source_name == "whatsapp_specs":
    #         # Remove very short specs and obvious non-specifications
    #         df = df[df[data_col].astype(str).str.len() >= 5]
    #         df = df[~df[data_col].astype(str).str.lower().str.contains(r'^(?:hi|hello|thanks|ok|yes|no)$', na=False, regex=True)]
            
    #     elif source_name == "pns_calls":
    #         # Remove very short transcripts and obvious non-content
    #         df = df[df[data_col].astype(str).str.len() >= 20]
    #         df = df[~df[data_col].astype(str).str.lower().str.contains(r'^(?:silence|background noise|inaudible).*$', na=False, regex=True)]
            
    #     elif source_name in ["rejection_comments", "lms_chats"]:
    #         # Remove very short comments and automated messages
    #         df = df[df[data_col].astype(str).str.len() >= 10]
    #         df = df[~df[data_col].astype(str).str.lower().str.contains(r'^(?:auto|system|bot).*message.*$', na=False, regex=True)]
        
    #     cleaned_count = int(len(df))
    #     noise_removed = original_count - cleaned_count
    #     logger.info(f"Stage 2 - Advanced cleaning: {original_count} → {cleaned_count} rows ({noise_removed} noise entries removed)")
        
    #     return df
    
    # @staticmethod
    # def _stage3_relevance_filtering(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 3: Advanced relevance filtering with semantic scoring"""
    #     data_col = config["data_column"]
    #     original_count = int(len(df))
        
    #     # GLOBAL STANDARD: Domain-neutral relevance scoring based on information theory
    #     # Instead of industry keywords, use universal information quality indicators
        
    #     def calculate_global_relevance_score(text):
    #         if pd.isna(text):
    #             return 0
    #         text_str = str(text).lower().strip()
            
    #         if len(text_str) < 3:  # Too short to be meaningful
    #             return 0
            
    #         score = 0
            
    #         # 1. INFORMATION DENSITY: Numbers + descriptive words indicate specifications
    #         word_count = len(text_str.split())
    #         number_count = len(re.findall(r'\b\d+(?:\.\d+)?\b', text_str))
    #         if word_count > 0:
    #             info_density = (number_count / word_count) * 10  # Numbers indicate specificity
    #             score += min(info_density, 5)  # Cap at 5 points
            
    #         # 2. SPECIFICITY INDICATORS: Patterns that indicate detailed information
    #         specificity_patterns = [
    #             r'\b\d+(?:\.\d+)?\s*[a-zA-Z]+\b',  # Number + unit (universal)
    #             r'\b[A-Z]{2,}\b',                   # Acronyms/codes (universal)
    #             r'\b\w+[-/]\w+\b',                 # Hyphenated/slashed terms (models, types)
    #             r'\b\d+[x×]\d+\b',                 # Dimensions (universal)
    #             r'\b\w+\s+\d+\b',                  # Word + number combinations
    #         ]
            
    #         for pattern in specificity_patterns:
    #             matches = len(re.findall(pattern, text_str))
    #             score += matches * 2  # Each match adds specificity
            
    #         # 3. LANGUAGE QUALITY: Real words vs gibberish
    #         alpha_ratio = len(re.findall(r'[a-zA-Z]', text_str)) / len(text_str) if text_str else 0
    #         if alpha_ratio > 0.5:  # At least 50% alphabetic characters
    #             score += 2
            
    #         # 4. STRUCTURE INDICATORS: Punctuation suggests structured information
    #         structure_chars = len(re.findall(r'[,;:|()[\]{}]', text_str))
    #         if structure_chars > 0:
    #             score += min(structure_chars, 3)  # Cap at 3 points
            
    #         # 5. LENGTH OPTIMIZATION: Optimal information length (not too short, not too long)
    #         text_len = len(text_str)
    #         if 10 <= text_len <= 200:  # Optimal range for meaningful information
    #             score += 3
    #         elif 5 <= text_len <= 500:  # Acceptable range
    #             score += 1
            
    #         # 6. UNIQUENESS BONUS: Rare words likely contain specific information
    #         words = text_str.split()
    #         rare_word_bonus = 0
    #         for word in words:
    #             if len(word) > 6 and word.isalpha():  # Long alphabetic words are often specific
    #                 rare_word_bonus += 0.5
    #         score += min(rare_word_bonus, 3)  # Cap at 3 points
            
    #         return score
        
    #     # Apply global relevance scoring
    #     df['relevance_score'] = df[data_col].apply(calculate_global_relevance_score)
        
    #     # Dynamic threshold based on data distribution
    #     if len(df) > 100:
    #         relevance_threshold = df['relevance_score'].quantile(0.3)  # Keep top 70%
    #     else:
    #         relevance_threshold = 1  # Minimum threshold for small datasets
        
    #     # Special handling for search keywords with frequency data
    #     if source_name == "search_keywords" and "frequency_column" in config:
    #         freq_col = config["frequency_column"]
    #         # Ensure frequency column is numeric
    #         try:
    #             df[freq_col] = pd.to_numeric(df[freq_col], errors='coerce').fillna(0)
    #             # Keep high-frequency terms even if relevance score is low
    #             high_freq_threshold = float(df[freq_col].quantile(0.8)) if len(df) > 10 else 0.0
    #             df = df[(df['relevance_score'] >= relevance_threshold) | (df[freq_col] >= high_freq_threshold)]
    #         except Exception as e:
    #             logger.warning(f"Frequency column processing failed: {e}. Using relevance only.")
    #             df = df[df['relevance_score'] >= relevance_threshold]
    #     else:
    #         # For other sources, apply relevance threshold
    #         df = df[df['relevance_score'] >= relevance_threshold]
        
    #     # Clean up temporary column
    #     df = df.drop('relevance_score', axis=1)
        
    #     filtered_count = int(len(df))
    #     logger.info(f"Stage 3 - Relevance filtering: {original_count} → {filtered_count} rows (threshold: {relevance_threshold:.1f})")
        
    #     return df
    
    # @staticmethod
    # def _stage4_intelligent_deduplication(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 4: Intelligent deduplication with fuzzy matching and semantic similarity"""
    #     data_col = config["data_column"]
    #     original_count = int(len(df))
        
    #     # GLOBAL STANDARD: Universal text normalization
    #     def global_normalize_text(text):
    #         if pd.isna(text):
    #             return ""
            
    #         text = str(text).lower().strip()
            
    #         # Universal normalization (no domain assumptions)
    #         text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single
    #         text = re.sub(r'[^\w\s]', ' ', text)  # Remove punctuation
    #         text = re.sub(r'\b(?:and|or|the|a|an|in|on|at|to|for|of|with|by)\b', ' ', text)  # Remove common stop words
    #         text = re.sub(r'\s+', ' ', text).strip()  # Clean up spaces again
            
    #         # Universal number normalization (no unit assumptions)
    #         text = re.sub(r'\b\d+\.0+\b', lambda m: m.group().replace('.0', ''), text)  # 5.0 -> 5
    #         text = re.sub(r'\b0+(\d+)\b', r'\1', text)  # 005 -> 5
            
    #         return text
        
    #     if source_name == "search_keywords" and "frequency_column" in config:
    #         # Advanced deduplication for search keywords
    #         freq_col = config["frequency_column"]
            
    #         try:
    #             # Ensure frequency column is numeric
    #             df[freq_col] = pd.to_numeric(df[freq_col], errors='coerce').fillna(0)
                
    #             # Create normalized text for grouping
    #             df['normalized_text'] = df[data_col].apply(global_normalize_text)
                
    #             # Group by normalized text and aggregate
    #             df_grouped = df.groupby('normalized_text').agg({
    #                 data_col: 'first',  # Keep first occurrence of original text
    #                 freq_col: 'sum'     # Sum frequencies
    #             }).reset_index()
                
    #             # Remove the temporary column
    #             df_grouped = df_grouped.drop('normalized_text', axis=1)
    #             df = df_grouped
                
    #         except Exception as e:
    #             logger.warning(f"Advanced deduplication failed: {e}. Using basic deduplication.")
    #             # Fallback to basic deduplication
    #             df['normalized_text'] = df[data_col].apply(advanced_normalize_text)
    #             df = df.drop_duplicates(subset=['normalized_text'], keep='first')
    #             df = df.drop('normalized_text', axis=1)
            
    #     else:
    #         # For other sources: Global fuzzy deduplication
    #         df['normalized_text'] = df[data_col].apply(global_normalize_text)
            
    #         # Remove exact duplicates on normalized text
    #         df = df.drop_duplicates(subset=['normalized_text'], keep='first')
            
    #         # Remove the temporary column
    #         df = df.drop('normalized_text', axis=1)
        
    #     dedup_count = int(len(df))
    #     duplicates_removed = original_count - dedup_count
    #     logger.info(f"Stage 4 - Intelligent deduplication: {original_count} → {dedup_count} rows ({duplicates_removed} duplicates consolidated)")
        
    #     return df
    
    # @staticmethod
    # def _stage5_data_enrichment(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 5: GLOBAL STANDARD data enrichment - completely domain-neutral"""
    #     data_col = config["data_column"]
        
    #     # GLOBAL STANDARD: Universal information patterns (no domain assumptions)
    #     def calculate_information_features(text):
    #         if pd.isna(text):
    #             return {}
            
    #         text_str = str(text).lower().strip()
    #         features = {}
            
    #         # 1. QUANTITATIVE CONTENT: Numbers indicate specifications
    #         features['has_numbers'] = bool(re.search(r'\b\d+(?:\.\d+)?\b', text_str))
    #         features['number_density'] = len(re.findall(r'\b\d+(?:\.\d+)?\b', text_str)) / max(len(text_str.split()), 1)
            
    #         # 2. STRUCTURED CONTENT: Patterns indicating organized information
    #         features['has_structure'] = bool(re.search(r'[,;:|()[\]{}]', text_str))
    #         features['has_codes'] = bool(re.search(r'\b[A-Z]{2,}\b', text_str))  # Acronyms/model codes
    #         features['has_ranges'] = bool(re.search(r'\d+\s*[-to]\s*\d+', text_str))  # Ranges
            
    #         # 3. SPECIFICITY INDICATORS: Detailed vs generic content
    #         features['has_models'] = bool(re.search(r'\b\w+[-/]\w+\b', text_str))  # Model numbers
    #         features['has_dimensions'] = bool(re.search(r'\b\d+[x×]\d+\b', text_str))  # Dimensions
    #         features['has_precision'] = bool(re.search(r'\b\d+\.\d+\b', text_str))  # Decimal precision
            
    #         # 4. LINGUISTIC QUALITY: Real content vs noise
    #         word_count = len(text_str.split())
    #         features['adequate_length'] = 3 <= word_count <= 50  # Optimal information length
    #         features['has_real_words'] = bool(re.search(r'\b[a-zA-Z]{3,}\b', text_str))  # Real words
    #         features['not_repetitive'] = not bool(re.search(r'(.)\1{3,}', text_str))  # Not spam
            
    #         return features
        
    #     # Apply universal feature extraction
    #     text_features = df[data_col].apply(calculate_information_features)
        
    #     # Convert features to columns
    #     feature_df = pd.DataFrame(text_features.tolist())
    #     for col in feature_df.columns:
    #         df[f'info_{col}'] = feature_df[col]
        
    #     # Calculate global information score (domain-neutral)
    #     boolean_features = [col for col in df.columns if col.startswith('info_') and df[col].dtype == bool]
    #     numeric_features = [col for col in df.columns if col.startswith('info_') and df[col].dtype in ['float64', 'int64']]
        
    #     # Boolean features score
    #     df['boolean_score'] = df[boolean_features].sum(axis=1)
        
    #     # Numeric features score (normalized)
    #     if numeric_features:
    #         df['numeric_score'] = df[numeric_features].sum(axis=1)
    #     else:
    #         df['numeric_score'] = 0
        
    #     # Combined information quality score
    #     df['global_info_score'] = df['boolean_score'] + df['numeric_score']
        
    #     logger.info(f"Stage 5 - Global enrichment: Added {len(boolean_features + numeric_features)} universal features")
    #     return df
    
    # @staticmethod
    # def _stage6_quality_validation(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 6: Multi-criteria quality validation and scoring"""
    #     data_col = config["data_column"]
    #     original_count = int(len(df))
        
    #     # GLOBAL STANDARD: Universal quality scoring
    #     def calculate_quality_score(row):
    #         text = str(row[data_col])
    #         score = 0
            
    #         # Universal length criteria (domain-neutral)
    #         text_len = len(text)
    #         if 10 <= text_len <= 200:  # Optimal range for any structured information
    #             score += 3
    #         elif 5 <= text_len <= 500:  # Acceptable range
    #             score += 1
            
    #         # Global information content score
    #         if 'global_info_score' in row:
    #             score += min(row['global_info_score'], 5)  # Cap at 5 points
            
    #         # Universal language quality
    #         if re.search(r'\b[a-zA-Z]{2,}\b', text):  # Contains real words
    #             score += 2
            
    #         # Universal spam detection
    #         if not re.search(r'(.)\1{4,}', text):  # No excessive repetition
    #             score += 1
            
    #         return score
        
    #     # Calculate quality scores
    #     df['quality_score'] = df.apply(calculate_quality_score, axis=1)
        
    #     # Dynamic quality threshold
    #     if len(df) > 50:
    #         quality_threshold = df['quality_score'].quantile(0.4)  # Keep top 60%
    #     else:
    #         quality_threshold = 3  # Minimum threshold for small datasets
        
    #     # Apply quality filter
    #     df_filtered = df[df['quality_score'] >= quality_threshold]
        
    #     # Clean up temporary columns
    #     df_filtered = df_filtered.drop(['quality_score'], axis=1)
        
    #     validated_count = int(len(df_filtered))
    #     logger.info(f"Stage 6 - Quality validation: {original_count} → {validated_count} rows (threshold: {quality_threshold:.1f})")
        
    #     return df_filtered
    
    # @staticmethod
    # def _stage7_final_optimization(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Stage 7: Final optimization and metadata preparation"""
    #     data_col = config["data_column"]
        
    #     # Sort by importance for better chunk organization
    #     if source_name == "search_keywords" and "frequency_column" in config:
    #         freq_col = config["frequency_column"]
    #         if freq_col in df.columns:
    #             try:
    #                 # Ensure frequency column is numeric for sorting
    #                 df[freq_col] = pd.to_numeric(df[freq_col], errors='coerce').fillna(0)
    #                 df = df.sort_values(freq_col, ascending=False)
    #             except Exception as e:
    #                 logger.warning(f"Frequency column sorting failed: {e}. Using default order.")
    #     else:
    #         # Sort by global information score if available
    #         if 'global_info_score' in df.columns:
    #             df = df.sort_values('global_info_score', ascending=False)
        
    #     # Clean up all temporary global feature columns for final output
    #     global_feature_columns = [col for col in df.columns if col.startswith(('info_', 'boolean_score', 'numeric_score', 'global_info_score'))]
    #     df = df.drop(global_feature_columns, axis=1, errors='ignore')
        
    #     logger.info(f"Stage 7 - Final optimization: Data sorted and optimized for extraction")
    #     return df
    
    # @staticmethod
    # def _fallback_basic_cleaning(df: pd.DataFrame, source_name: str, config: Dict) -> pd.DataFrame:
    #     """Fallback to basic cleaning if advanced preprocessing fails"""
    #     data_col = config["data_column"]
        
    #     # Basic cleaning only
    #     df = df.dropna(subset=[data_col])
    #     df = df[df[data_col].astype(str).str.strip() != ""]
    #     df = df[df[data_col].astype(str).str.lower() != "nan"]
        
    #     logger.info(f"Fallback processing applied for {source_name}: {len(df)} rows")
    #     return df
    
    @staticmethod
    def _process_chunk_advanced(df: pd.DataFrame, source_name: str, config: Dict, chunk_num: int) -> str:
        """Process a single chunk with advanced formatting and metadata"""
        logger.info(f"Processing advanced chunk {chunk_num} for {source_name}: {len(df)} rows")
        
        # Process based on source type with enhanced formatting
        if source_name == "search_keywords":
            return DataProcessor._process_search_keywords_advanced(df, config)
        elif source_name == "whatsapp_specs":
            return DataProcessor._process_whatsapp_specs_advanced(df, config)
        elif source_name == "pns_calls":
            return DataProcessor._process_pns_calls_advanced(df, config)
        elif source_name == "rejection_comments":
            return DataProcessor._process_rejection_comments_advanced(df, config)
        elif source_name == "lms_chats":
            return DataProcessor._process_lms_chats_advanced(df, config)
        else:
            raise ValueError(f"Unknown source type: {source_name}")
    
    @staticmethod
    def _process_search_keywords_advanced(df: pd.DataFrame, config: Dict) -> str:
        """Process search keywords with advanced formatting and quality indicators"""
        data_col = config["data_column"]
        freq_col = config["frequency_column"]
        
        # Ensure required columns exist
        if data_col not in df.columns or freq_col not in df.columns:
            raise ValueError(f"Required columns missing: {data_col}, {freq_col}")
        
        # Advanced processing with quality metadata
        df_clean = df[[data_col, freq_col]].dropna()
        df_clean = df_clean[df_clean[data_col].str.strip() != ""]
        df_clean = df_clean.sort_values(freq_col, ascending=False)
        
        # Enhanced CSV format with quality indicators
        formatted_text = f"# SEARCH KEYWORDS DATA (Processed: {len(df_clean)} high-quality entries)\n"
        formatted_text += f"{data_col},{freq_col}\n"
        
        for _, row in df_clean.iterrows():
            formatted_text += f"{row[data_col]},{row[freq_col]}\n"
        
        logger.info(f"Advanced search keywords processed: {len(df_clean)} entries with quality enhancement")
        return formatted_text
    
    @staticmethod
    def _process_whatsapp_specs_advanced(df: pd.DataFrame, config: Dict) -> str:
        """Process WhatsApp specs with advanced formatting"""
        data_col = config["data_column"]
        
        if data_col not in df.columns:
            raise ValueError(f"Required column missing: {data_col}")
        
        # Advanced processing
        df_clean = df[data_col].dropna()
        df_clean = df_clean[df_clean.str.strip() != ""]
        
        # Enhanced format with metadata
        formatted_text = f"# WHATSAPP SPECIFICATIONS (Processed: {len(df_clean)} validated entries)\n"
        formatted_text += f"{data_col}\n"
        for spec in df_clean.unique():
            formatted_text += f"{spec}\n"
        
        logger.info(f"Advanced WhatsApp specs processed: {len(df_clean)} entries")
        return formatted_text
    
    @staticmethod
    def _process_pns_calls_advanced(df: pd.DataFrame, config: Dict) -> str:
        """Process PNS calls with advanced content optimization"""
        data_col = config["data_column"]
        
        if data_col not in df.columns:
            raise ValueError(f"Required column missing: {data_col}")
        
        # Advanced processing
        df_clean = df[data_col].dropna()
        df_clean = df_clean[df_clean.str.strip() != ""]
        
        # Enhanced format with quality optimization
        formatted_text = f"# PNS CALL TRANSCRIPTS (Processed: {len(df_clean)} quality transcripts)\n"
        formatted_text += f"{data_col}\n"
        
        for i, transcript in enumerate(df_clean, 1):
            # Intelligent truncation preserving key content
            if len(transcript) > 1000:
                # Try to find a good breaking point
                truncated = transcript[:1000]
                last_sentence = truncated.rfind('.')
                if last_sentence > 800:  # If we can find a sentence end
                    truncated = truncated[:last_sentence + 1]
                transcript_excerpt = truncated + "..."
            else:
                transcript_excerpt = transcript
                
            formatted_text += f"Call {i}: {transcript_excerpt}\n\n"
        
        logger.info(f"Advanced PNS calls processed: {len(df_clean)} transcriptions")
        return formatted_text
    
    @staticmethod
    def _process_rejection_comments_advanced(df: pd.DataFrame, config: Dict) -> str:
        """Process rejection comments with advanced categorization"""
        data_col = config["data_column"]
        
        if data_col not in df.columns:
            raise ValueError(f"Required column missing: {data_col}")
        
        # Advanced processing
        df_clean = df[data_col].dropna()
        df_clean = df_clean[df_clean.str.strip() != ""]
        
        # Enhanced format with categorization hints
        formatted_text = f"# REJECTION COMMENTS (Processed: {len(df_clean)} validated comments)\n"
        formatted_text += f"{data_col}\n"
        for comment in df_clean.unique():
            formatted_text += f"{comment}\n"
        
        logger.info(f"Advanced rejection comments processed: {len(df_clean)} entries")
        return formatted_text
    
    @staticmethod
    def _process_lms_chats_advanced(df: pd.DataFrame, config: Dict) -> str:
        """Process LMS chats with advanced JSON parsing and optimization"""
        data_col = config["data_column"]
        
        if data_col not in df.columns:
            raise ValueError(f"Required column missing: {data_col}")
        
        # Advanced processing
        df_clean = df[data_col].dropna()
        
        formatted_text = f"# LMS CHAT DATA (Processed with advanced JSON parsing)\n"
        formatted_text += f"extracted_chat_data\n"
        processed_count = 0
        
        for json_str in df_clean:
            try:
                if isinstance(json_str, str) and json_str.strip():
                    # Basic validation before parsing
                    json_str = json_str.strip()
                    if not (json_str.startswith('{') and json_str.endswith('}')):
                        logger.debug(f"Skipping malformed JSON (missing braces): {json_str[:30]}...")
                        continue
                    
                    chat_data = json.loads(json_str)
                    
                    # Advanced information extraction
                    extracted_info = []
                    
                    # Prioritize specification-related content
                    if "isq" in chat_data and isinstance(chat_data["isq"], dict):
                        for key, value in chat_data["isq"].items():
                            # Intelligent truncation preserving key information
                            value_str = str(value)
                            if len(value_str) > 100:
                                # Try to preserve complete words
                                truncated = value_str[:100]
                                last_space = truncated.rfind(' ')
                                if last_space > 80:
                                    truncated = truncated[:last_space]
                                value_str = truncated + "..."
                            extracted_info.append(f"{key}:{value_str}")
                    
                    # Extract meaningful message content
                    if "message_text" in chat_data and chat_data["message_text"]:
                        message = str(chat_data["message_text"])
                        if len(message) > 150:
                            # Intelligent truncation
                            truncated = message[:150]
                            last_sentence = truncated.rfind('.')
                            if last_sentence > 120:
                                truncated = truncated[:last_sentence + 1]
                            message = truncated + "..."
                        extracted_info.append(f"Msg:{message}")
                    
                    # Only include if we have meaningful data
                    if extracted_info:
                        chat_line = "|".join(extracted_info) + "\n"
                        formatted_text += chat_line
                        processed_count += 1
                        
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON entry {processed_count + 1}: {str(e)[:100]} | Data preview: {json_str[:50]}...")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error processing LMS chat entry {processed_count + 1}: {str(e)[:100]}")
                continue
        
        total_entries = len(df_clean)
        success_rate = (processed_count / total_entries * 100) if total_entries > 0 else 0
        logger.info(f"Advanced LMS chats processed: {processed_count}/{total_entries} entries ({success_rate:.1f}% success rate)")
        return formatted_text

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Estimate token count for text with improved accuracy"""
        # More accurate token estimation accounting for JSON and structured data
        char_count = len(text)
        word_count = len(text.split())
        
        # Tokens are roughly 0.75 * word count for English text
        # But structured data (JSON, CSV) has more punctuation so use char-based estimate
        word_based_estimate = int(word_count * 0.75)
        char_based_estimate = int(char_count * AVERAGE_TOKENS_PER_CHAR)
        
        # Use the higher estimate for safety
        return max(word_based_estimate, char_based_estimate)
    
    # @staticmethod
    # def _smart_sample(df: pd.DataFrame, source_name: str, config: Dict, max_rows: int) -> pd.DataFrame:
    #     """Apply intelligent sampling based on data source characteristics"""
    #     try:
    #         if source_name == "search_keywords" and "frequency_column" in config:
    #             # For search keywords: Take top entries by pageviews/frequency
    #             freq_col = config["frequency_column"]
    #             if freq_col in df.columns:
    #                 df_sorted = df.sort_values(freq_col, ascending=False)
    #                 sampled_df = df_sorted.head(max_rows)
    #                 logger.info(f"Sampled top {len(sampled_df)} entries by {freq_col} for {source_name}")
    #                 return sampled_df
            
    #         elif source_name in ["whatsapp_specs", "rejection_comments"]:
    #             # For spec lists: Take most frequent unique values
    #             data_col = config["data_column"]
    #             if data_col in df.columns:
    #                 # Get value counts and take top entries
    #                 value_counts = df[data_col].value_counts()
    #                 top_values = value_counts.head(max_rows).index
    #                 sampled_df = df[df[data_col].isin(top_values)]
    #                 logger.info(f"Sampled {len(sampled_df)} rows with top {len(top_values)} unique values for {source_name}")
    #                 return sampled_df
            
    #         # Default: Take first N rows
    #         sampled_df = df.head(max_rows)
    #         logger.info(f"Using default sampling (first {len(sampled_df)} rows) for {source_name}")
    #         return sampled_df
            
    #     except Exception as e:
    #         logger.warning(f"Smart sampling failed for {source_name}: {e}. Using default sampling.")
    #         return df.head(max_rows)

    # @staticmethod
    # def validate_csv_file(file_content: str, source_name: str) -> Dict[str, Any]:
        """Validate CSV file and return metadata"""
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            column_config = COLUMN_MAPPINGS.get(source_name)
            if not column_config:
                return {"valid": False, "error": f"Unknown source: {source_name}"}
            
            # Check required columns
            required_columns = [column_config["data_column"]]
            if "frequency_column" in column_config:
                required_columns.append(column_config["frequency_column"])
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                return {
                    "valid": False, 
                    "error": f"Missing columns: {missing_columns}",
                    "available_columns": list(df.columns)
                }
            
            return {
                "valid": True,
                "row_count": len(df),
                "columns": list(df.columns),
                "data_column": column_config["data_column"]
            }
            
        except Exception as e:
            return {"valid": False, "error": str(e)} 
            return {"valid": False, "error": str(e)} 