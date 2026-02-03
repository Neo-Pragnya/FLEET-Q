"""
DuckDB Utilities for FLEET-Q

Comprehensive data processing utilities leveraging DuckDB, Polars, and fuzzy matching
for high-performance data operations in Kubernetes pods.

Features:
- Snowflake ↔ Parquet/JSON data transfer
- Fuzzy matching (Levenshtein, Jaro-Winkler, cosine similarity)
- Exact matching with indexing
- Polars integration for fast data processing
- Memory-efficient streaming operations
- Pod-aware resource management

Requirements:
    pip install duckdb polars pyarrow snowflake-connector-python rapidfuzz

Usage:
    from fleet_q.duckdb_utils import DuckDBDataManager
    
    # Initialize with adaptive resource limits
    manager = DuckDBDataManager(db_path=":memory:")
    
    # Load from Snowflake
    df = manager.load_from_snowflake(
        query="SELECT * FROM my_table",
        connection_params={...}
    )
    
    # Fuzzy match
    results = manager.fuzzy_match(
        source_df=df,
        target_df=other_df,
        source_col="name",
        target_col="company_name",
        method="jaro_winkler",
        threshold=0.85
    )
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Try to import fuzzy matching libraries
try:
    from rapidfuzz import fuzz, distance
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

# Try to import Snowflake connector
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

# Try to import pod resource detection
try:
    from fleet_q.cgroup_aware_resources import effective_memory_gb, effective_cpu_cores
    POD_RESOURCES_AVAILABLE = True
except ImportError:
    POD_RESOURCES_AVAILABLE = False


# ============================================================================
# Configuration and Types
# ============================================================================

MatchMethod = Literal[
    "levenshtein",
    "jaro",
    "jaro_winkler",
    "token_sort_ratio",
    "token_set_ratio",
    "partial_ratio",
    "cosine"
]


@dataclass
class MatchResult:
    """Result of a fuzzy match operation"""
    source_id: Any
    target_id: Any
    source_value: str
    target_value: str
    score: float
    method: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DuckDBConfig:
    """DuckDB configuration optimized for pod resources"""
    memory_limit: Optional[str] = None  # e.g., "4GB"
    threads: Optional[int] = None
    temp_directory: Optional[str] = None
    enable_object_cache: bool = True
    
    @classmethod
    def from_pod_resources(cls) -> DuckDBConfig:
        """Auto-configure based on detected pod resources"""
        config = cls()
        
        if POD_RESOURCES_AVAILABLE:
            # Use 50% of available memory for DuckDB
            memory_gb = effective_memory_gb()
            if memory_gb:
                duckdb_memory = memory_gb * 0.5
                config.memory_limit = f"{int(duckdb_memory)}GB"
            
            # Use 75% of available cores
            cores = effective_cpu_cores()
            config.threads = max(1, int(cores * 0.75))
        
        return config


# ============================================================================
# Main DuckDB Data Manager
# ============================================================================

class DuckDBDataManager:
    """
    Comprehensive data manager using DuckDB, Polars, and fuzzy matching.
    
    Handles:
    - Snowflake data transfer (Parquet/JSON)
    - Fuzzy and exact matching
    - High-performance data transformations
    - Memory-efficient operations
    """
    
    def __init__(
        self,
        db_path: str = ":memory:",
        config: Optional[DuckDBConfig] = None,
        auto_configure: bool = True
    ):
        """
        Initialize DuckDB data manager.
        
        Args:
            db_path: Path to DuckDB database (":memory:" for in-memory)
            config: DuckDB configuration (auto-detected if None and auto_configure=True)
            auto_configure: Auto-detect pod resources for configuration
        """
        self.db_path = db_path
        
        # Auto-configure from pod resources
        if config is None and auto_configure:
            config = DuckDBConfig.from_pod_resources()
        
        self.config = config or DuckDBConfig()
        
        # Initialize DuckDB connection
        self.con = self._create_connection()
        
        # Install and load extensions
        self._setup_extensions()
    
    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection with optimized settings"""
        con = duckdb.connect(self.db_path)
        
        # Apply configuration
        if self.config.memory_limit:
            con.execute(f"SET memory_limit='{self.config.memory_limit}'")
        
        if self.config.threads:
            con.execute(f"SET threads={self.config.threads}")
        
        if self.config.temp_directory:
            con.execute(f"SET temp_directory='{self.config.temp_directory}'")
        
        con.execute(f"SET enable_object_cache={self.config.enable_object_cache}")
        
        return con
    
    def _setup_extensions(self):
        """Install and load required DuckDB extensions"""
        extensions = ["parquet", "json"]
        
        for ext in extensions:
            try:
                self.con.execute(f"INSTALL {ext}")
                self.con.execute(f"LOAD {ext}")
            except Exception as e:
                print(f"Warning: Could not load extension {ext}: {e}")
    
    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # ========================================================================
    # Snowflake Integration
    # ========================================================================
    
    def load_from_snowflake(
        self,
        query: str,
        connection_params: Dict[str, str],
        to_parquet: Optional[str] = None,
        chunk_size: int = 100_000
    ) -> pl.DataFrame:
        """
        Load data from Snowflake into Polars DataFrame.
        
        Args:
            query: SQL query to execute
            connection_params: Snowflake connection parameters
            to_parquet: Optional path to save as Parquet
            chunk_size: Number of rows per chunk
        
        Returns:
            Polars DataFrame
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("snowflake-connector-python not installed")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**connection_params)
        cursor = conn.cursor()
        
        try:
            # Execute query
            cursor.execute(query)
            
            # Fetch in chunks and collect
            chunks = []
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Create Polars DataFrame from chunk
                chunk_df = pl.DataFrame(
                    {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                )
                chunks.append(chunk_df)
            
            # Concatenate all chunks
            df = pl.concat(chunks) if chunks else pl.DataFrame()
            
            # Save to Parquet if requested
            if to_parquet:
                df.write_parquet(to_parquet, compression="zstd")
            
            return df
        
        finally:
            cursor.close()
            conn.close()
    
    def upload_to_snowflake(
        self,
        df: Union[pl.DataFrame, pa.Table, str],
        table_name: str,
        connection_params: Dict[str, str],
        schema: Optional[str] = None,
        if_exists: Literal["fail", "replace", "append"] = "fail",
        chunk_size: int = 100_000
    ):
        """
        Upload data to Snowflake from Polars DataFrame, Arrow Table, or Parquet file.
        
        Args:
            df: Polars DataFrame, Arrow Table, or path to Parquet file
            table_name: Target table name
            connection_params: Snowflake connection parameters
            schema: Schema name (uses default if None)
            if_exists: What to do if table exists
            chunk_size: Number of rows per chunk
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("snowflake-connector-python not installed")
        
        # Load data if path provided
        if isinstance(df, str):
            df = pl.read_parquet(df)
        elif isinstance(df, pa.Table):
            df = pl.from_arrow(df)
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**connection_params)
        cursor = conn.cursor()
        
        try:
            # Prepare table name
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Handle if_exists
            if if_exists == "replace":
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            elif if_exists == "fail":
                cursor.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_name = '{table_name}'"
                )
                if cursor.fetchone()[0] > 0:
                    raise ValueError(f"Table {full_table_name} already exists")
            
            # Create table from first chunk
            first_chunk = df.head(1)
            create_sql = self._generate_create_table_sql(first_chunk, full_table_name)
            
            if if_exists != "append":
                cursor.execute(create_sql)
            
            # Insert data in chunks
            columns = df.columns
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            for i in range(0, len(df), chunk_size):
                chunk = df.slice(i, chunk_size)
                rows = [tuple(row) for row in chunk.iter_rows()]
                cursor.executemany(insert_sql, rows)
            
            conn.commit()
        
        finally:
            cursor.close()
            conn.close()
    
    def _generate_create_table_sql(self, df: pl.DataFrame, table_name: str) -> str:
        """Generate CREATE TABLE SQL from Polars DataFrame schema"""
        type_mapping = {
            pl.Int8: "SMALLINT",
            pl.Int16: "SMALLINT",
            pl.Int32: "INTEGER",
            pl.Int64: "BIGINT",
            pl.UInt8: "SMALLINT",
            pl.UInt16: "INTEGER",
            pl.UInt32: "BIGINT",
            pl.UInt64: "BIGINT",
            pl.Float32: "FLOAT",
            pl.Float64: "DOUBLE",
            pl.Boolean: "BOOLEAN",
            pl.Utf8: "VARCHAR",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
        }
        
        columns = []
        for col, dtype in df.schema.items():
            sf_type = type_mapping.get(dtype, "VARCHAR")
            columns.append(f"{col} {sf_type}")
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    # ========================================================================
    # Parquet and JSON Operations
    # ========================================================================
    
    def load_parquet(self, path: str, lazy: bool = False) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Load Parquet file into Polars DataFrame.
        
        Args:
            path: Path to Parquet file
            lazy: Return LazyFrame for deferred execution
        
        Returns:
            Polars DataFrame or LazyFrame
        """
        if lazy:
            return pl.scan_parquet(path)
        return pl.read_parquet(path)
    
    def load_json(
        self,
        path: str,
        json_format: Literal["lines", "array"] = "lines"
    ) -> pl.DataFrame:
        """
        Load JSON file into Polars DataFrame.
        
        Args:
            path: Path to JSON file
            json_format: "lines" for JSONL, "array" for JSON array
        
        Returns:
            Polars DataFrame
        """
        if json_format == "lines":
            return pl.read_ndjson(path)
        else:
            return pl.read_json(path)
    
    def save_parquet(
        self,
        df: pl.DataFrame,
        path: str,
        compression: str = "zstd",
        row_group_size: Optional[int] = None
    ):
        """
        Save Polars DataFrame to Parquet file.
        
        Args:
            df: Polars DataFrame
            path: Output path
            compression: Compression algorithm (zstd, snappy, gzip)
            row_group_size: Number of rows per row group
        """
        df.write_parquet(
            path,
            compression=compression,
            row_group_size=row_group_size
        )
    
    def save_json(
        self,
        df: pl.DataFrame,
        path: str,
        json_format: Literal["lines", "array"] = "lines"
    ):
        """
        Save Polars DataFrame to JSON file.
        
        Args:
            df: Polars DataFrame
            path: Output path
            json_format: "lines" for JSONL, "array" for JSON array
        """
        if json_format == "lines":
            df.write_ndjson(path)
        else:
            df.write_json(path)
    
    # ========================================================================
    # DuckDB Query Operations
    # ========================================================================
    
    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute SQL query and return Polars DataFrame.
        
        Args:
            sql: SQL query
        
        Returns:
            Polars DataFrame
        """
        result = self.con.execute(sql).arrow()
        return pl.from_arrow(result)
    
    def register_dataframe(self, df: pl.DataFrame, name: str):
        """
        Register Polars DataFrame as DuckDB table.
        
        Args:
            df: Polars DataFrame
            name: Table name
        """
        self.con.register(name, df.to_arrow())
    
    def register_parquet(self, path: str, name: str):
        """
        Register Parquet file as DuckDB table (zero-copy).
        
        Args:
            path: Path to Parquet file
            name: Table name
        """
        self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
    
    # ========================================================================
    # Exact Matching
    # ========================================================================
    
    def exact_match(
        self,
        source_df: pl.DataFrame,
        target_df: pl.DataFrame,
        source_col: str,
        target_col: str,
        source_id: str = "id",
        target_id: str = "id",
        case_sensitive: bool = False
    ) -> pl.DataFrame:
        """
        Perform exact matching between two DataFrames.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            source_col: Column to match in source
            target_col: Column to match in target
            source_id: ID column in source
            target_id: ID column in target
            case_sensitive: Whether to match case-sensitively
        
        Returns:
            DataFrame with matched pairs
        """
        # Register DataFrames
        self.register_dataframe(source_df, "source")
        self.register_dataframe(target_df, "target")
        
        # Build SQL query
        if case_sensitive:
            condition = f"source.{source_col} = target.{target_col}"
        else:
            condition = f"LOWER(source.{source_col}) = LOWER(target.{target_col})"
        
        sql = f"""
            SELECT 
                source.{source_id} as source_id,
                target.{target_id} as target_id,
                source.{source_col} as source_value,
                target.{target_col} as target_value,
                1.0 as score,
                'exact' as method
            FROM source
            INNER JOIN target ON {condition}
        """
        
        return self.query(sql)
    
    # ========================================================================
    # Fuzzy Matching
    # ========================================================================
    
    def fuzzy_match(
        self,
        source_df: pl.DataFrame,
        target_df: pl.DataFrame,
        source_col: str,
        target_col: str,
        source_id: str = "id",
        target_id: str = "id",
        method: MatchMethod = "jaro_winkler",
        threshold: float = 0.8,
        top_k: int = 1,
        batch_size: int = 10_000
    ) -> List[MatchResult]:
        """
        Perform fuzzy matching between two DataFrames.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            source_col: Column to match in source
            target_col: Column to match in target
            source_id: ID column in source
            target_id: ID column in target
            method: Matching algorithm
            threshold: Minimum similarity score (0-1)
            top_k: Number of top matches per source record
            batch_size: Number of source records to process at once
        
        Returns:
            List of MatchResult objects
        """
        if not RAPIDFUZZ_AVAILABLE:
            raise ImportError("rapidfuzz not installed. Install: pip install rapidfuzz")
        
        # Extract columns as Python lists for processing
        source_data = source_df.select([source_id, source_col]).to_dicts()
        target_data = target_df.select([target_id, target_col]).to_dicts()
        
        target_values = [row[target_col] for row in target_data]
        target_ids = [row[target_id] for row in target_data]
        
        results = []
        
        # Process in batches
        for i in range(0, len(source_data), batch_size):
            batch = source_data[i:i + batch_size]
            
            for source_row in batch:
                source_value = source_row[source_col]
                if not source_value or not isinstance(source_value, str):
                    continue
                
                # Calculate similarity scores
                scores = self._calculate_similarity(
                    source_value,
                    target_values,
                    method
                )
                
                # Get top-k matches above threshold
                top_matches = sorted(
                    [(idx, score) for idx, score in enumerate(scores) if score >= threshold],
                    key=lambda x: x[1],
                    reverse=True
                )[:top_k]
                
                # Create MatchResult objects
                for target_idx, score in top_matches:
                    results.append(MatchResult(
                        source_id=source_row[source_id],
                        target_id=target_ids[target_idx],
                        source_value=source_value,
                        target_value=target_values[target_idx],
                        score=score,
                        method=method
                    ))
        
        return results
    
    def _calculate_similarity(
        self,
        source: str,
        targets: List[str],
        method: MatchMethod
    ) -> List[float]:
        """
        Calculate similarity scores between source and list of targets.
        
        Args:
            source: Source string
            targets: List of target strings
            method: Matching algorithm
        
        Returns:
            List of similarity scores (0-1)
        """
        if method == "levenshtein":
            scores = [
                1.0 - (distance.Levenshtein.normalized_distance(source, t) if t else 1.0)
                for t in targets
            ]
        elif method == "jaro":
            scores = [
                distance.Jaro.normalized_similarity(source, t) if t else 0.0
                for t in targets
            ]
        elif method == "jaro_winkler":
            scores = [
                distance.JaroWinkler.normalized_similarity(source, t) if t else 0.0
                for t in targets
            ]
        elif method == "token_sort_ratio":
            scores = [
                fuzz.token_sort_ratio(source, t) / 100.0 if t else 0.0
                for t in targets
            ]
        elif method == "token_set_ratio":
            scores = [
                fuzz.token_set_ratio(source, t) / 100.0 if t else 0.0
                for t in targets
            ]
        elif method == "partial_ratio":
            scores = [
                fuzz.partial_ratio(source, t) / 100.0 if t else 0.0
                for t in targets
            ]
        elif method == "cosine":
            scores = self._cosine_similarity_batch(source, targets)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return scores
    
    def _cosine_similarity_batch(self, source: str, targets: List[str]) -> List[float]:
        """Calculate cosine similarity using character n-grams"""
        from collections import Counter
        import math
        
        def get_ngrams(text: str, n: int = 2) -> Counter:
            """Get character n-grams"""
            text = text.lower()
            return Counter([text[i:i+n] for i in range(len(text) - n + 1)])
        
        source_ngrams = get_ngrams(source)
        source_norm = math.sqrt(sum(c * c for c in source_ngrams.values()))
        
        scores = []
        for target in targets:
            if not target:
                scores.append(0.0)
                continue
            
            target_ngrams = get_ngrams(target)
            target_norm = math.sqrt(sum(c * c for c in target_ngrams.values()))
            
            if source_norm == 0 or target_norm == 0:
                scores.append(0.0)
                continue
            
            # Dot product
            dot_product = sum(
                source_ngrams[ng] * target_ngrams[ng]
                for ng in source_ngrams
                if ng in target_ngrams
            )
            
            similarity = dot_product / (source_norm * target_norm)
            scores.append(similarity)
        
        return scores
    
    def fuzzy_match_polars(
        self,
        source_df: pl.DataFrame,
        target_df: pl.DataFrame,
        source_col: str,
        target_col: str,
        method: MatchMethod = "jaro_winkler",
        threshold: float = 0.8
    ) -> pl.DataFrame:
        """
        Fuzzy match and return results as Polars DataFrame.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            source_col: Column to match in source
            target_col: Column to match in target
            method: Matching algorithm
            threshold: Minimum similarity score
        
        Returns:
            Polars DataFrame with match results
        """
        results = self.fuzzy_match(
            source_df=source_df,
            target_df=target_df,
            source_col=source_col,
            target_col=target_col,
            method=method,
            threshold=threshold
        )
        
        # Convert to Polars DataFrame
        return pl.DataFrame({
            "source_id": [r.source_id for r in results],
            "target_id": [r.target_id for r in results],
            "source_value": [r.source_value for r in results],
            "target_value": [r.target_value for r in results],
            "score": [r.score for r in results],
            "method": [r.method for r in results]
        })
    
    # ========================================================================
    # Advanced Operations
    # ========================================================================
    
    def deduplicate(
        self,
        df: pl.DataFrame,
        columns: List[str],
        method: Literal["exact", "fuzzy"] = "exact",
        fuzzy_threshold: float = 0.9
    ) -> pl.DataFrame:
        """
        Deduplicate DataFrame using exact or fuzzy matching.
        
        Args:
            df: DataFrame to deduplicate
            columns: Columns to consider for deduplication
            method: "exact" or "fuzzy"
            fuzzy_threshold: Threshold for fuzzy matching
        
        Returns:
            Deduplicated DataFrame
        """
        if method == "exact":
            return df.unique(subset=columns, keep="first")
        
        else:  # fuzzy
            # Add row IDs
            df_with_id = df.with_row_count("_row_id")
            
            # Fuzzy match against itself
            matches = self.fuzzy_match(
                source_df=df_with_id,
                target_df=df_with_id,
                source_col=columns[0],  # Use first column
                target_col=columns[0],
                source_id="_row_id",
                target_id="_row_id",
                threshold=fuzzy_threshold
            )
            
            # Build duplicate groups
            duplicates = set()
            for match in matches:
                if match.source_id != match.target_id:
                    # Keep the row with smaller ID
                    duplicates.add(max(match.source_id, match.target_id))
            
            # Filter out duplicates
            return df_with_id.filter(~pl.col("_row_id").is_in(list(duplicates))).drop("_row_id")
    
    def aggregate_json(
        self,
        df: pl.DataFrame,
        group_by: List[str],
        json_col: str,
        output_col: str = "aggregated_json"
    ) -> pl.DataFrame:
        """
        Aggregate JSON objects by grouping.
        
        Args:
            df: DataFrame with JSON column
            group_by: Columns to group by
            json_col: Column containing JSON strings/dicts
            output_col: Name for aggregated column
        
        Returns:
            DataFrame with aggregated JSON
        """
        return df.groupby(group_by).agg(
            pl.col(json_col).alias(output_col)
        )
    
    def explode_json(
        self,
        df: pl.DataFrame,
        json_col: str,
        schema: Optional[Dict[str, pl.DataType]] = None
    ) -> pl.DataFrame:
        """
        Explode JSON column into separate columns.
        
        Args:
            df: DataFrame with JSON column
            json_col: Column containing JSON strings
            schema: Optional schema for JSON fields
        
        Returns:
            DataFrame with exploded JSON columns
        """
        # Parse JSON if string
        if df[json_col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(json_col).str.json_extract().alias(json_col)
            )
        
        # Explode struct column
        return df.unnest(json_col)


# ============================================================================
# Utility Functions
# ============================================================================

def create_fuzzy_index(
    df: pl.DataFrame,
    column: str,
    ngram_size: int = 3
) -> Dict[str, List[int]]:
    """
    Create n-gram index for faster fuzzy matching.
    
    Args:
        df: DataFrame
        column: Column to index
        ngram_size: Size of n-grams
    
    Returns:
        Dictionary mapping n-grams to row indices
    """
    from collections import defaultdict
    
    index = defaultdict(list)
    
    for idx, value in enumerate(df[column]):
        if not value or not isinstance(value, str):
            continue
        
        value = value.lower()
        for i in range(len(value) - ngram_size + 1):
            ngram = value[i:i + ngram_size]
            index[ngram].append(idx)
    
    return dict(index)


def search_fuzzy_index(
    query: str,
    index: Dict[str, List[int]],
    ngram_size: int = 3,
    min_overlap: int = 1
) -> List[int]:
    """
    Search fuzzy index for candidate matches.
    
    Args:
        query: Query string
        index: N-gram index
        ngram_size: Size of n-grams
        min_overlap: Minimum number of overlapping n-grams
    
    Returns:
        List of candidate row indices
    """
    from collections import Counter
    
    query = query.lower()
    query_ngrams = [query[i:i + ngram_size] for i in range(len(query) - ngram_size + 1)]
    
    # Count overlaps
    candidate_counts = Counter()
    for ngram in query_ngrams:
        if ngram in index:
            for idx in index[ngram]:
                candidate_counts[idx] += 1
    
    # Filter by minimum overlap
    return [idx for idx, count in candidate_counts.items() if count >= min_overlap]


# ============================================================================
# Example Usage
# ============================================================================

def example_usage():
    """Example demonstrating DuckDB utilities"""
    
    # Initialize manager with auto-detected resources
    manager = DuckDBDataManager(auto_configure=True)
    
    # Example 1: Load from Snowflake and save as Parquet
    print("Example 1: Load from Snowflake")
    # df = manager.load_from_snowflake(
    #     query="SELECT * FROM my_table LIMIT 10000",
    #     connection_params={
    #         "account": "your_account",
    #         "user": "your_user",
    #         "password": "your_password",
    #         "database": "your_db",
    #         "schema": "your_schema",
    #         "warehouse": "your_warehouse"
    #     },
    #     to_parquet="/tmp/data.parquet"
    # )
    
    # Example 2: Create sample data for fuzzy matching
    print("\nExample 2: Fuzzy Matching")
    source_df = pl.DataFrame({
        "id": [1, 2, 3],
        "company_name": ["Apple Inc", "Microsoft Corp", "Google LLC"]
    })
    
    target_df = pl.DataFrame({
        "id": [101, 102, 103, 104],
        "vendor_name": ["Apple Incorporated", "Microsft Corporation", "Google", "Amazon"]
    })
    
    # Fuzzy match using Jaro-Winkler
    matches = manager.fuzzy_match(
        source_df=source_df,
        target_df=target_df,
        source_col="company_name",
        target_col="vendor_name",
        method="jaro_winkler",
        threshold=0.7
    )
    
    print(f"Found {len(matches)} matches:")
    for match in matches:
        print(f"  {match.source_value} → {match.target_value} (score: {match.score:.3f})")
    
    # Example 3: Exact matching with DuckDB
    print("\nExample 3: Exact Matching")
    exact_matches = manager.exact_match(
        source_df=source_df,
        target_df=target_df,
        source_col="company_name",
        target_col="vendor_name",
        case_sensitive=False
    )
    print(exact_matches)
    
    # Example 4: Deduplication
    print("\nExample 4: Deduplication")
    duplicates_df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Apple Inc", "Apple Incorporated", "Microsoft", "Microsft"]
    })
    
    deduped = manager.deduplicate(
        df=duplicates_df,
        columns=["name"],
        method="fuzzy",
        fuzzy_threshold=0.85
    )
    print(f"Original: {len(duplicates_df)} rows, Deduped: {len(deduped)} rows")
    
    manager.close()


if __name__ == "__main__":
    example_usage()
