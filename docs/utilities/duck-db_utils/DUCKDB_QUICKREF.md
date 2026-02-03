# DuckDB Utilities - Quick Reference

**Last Updated:** 2025-02-02

## 🚀 Installation

```bash
# Core dependencies
pip install duckdb polars pyarrow

# Fuzzy matching
pip install rapidfuzz

# Snowflake integration (optional)
pip install snowflake-connector-python
```

## 💡 Quick Start

```python
from fleet_q.duckdb_utils import DuckDBDataManager

# Initialize (auto-configures from pod resources)
with DuckDBDataManager(auto_configure=True) as manager:
    # Your code here
    pass
```

## 📊 Data Loading

| Operation | Code |
|-----------|------|
| **Load Parquet** | `df = manager.load_parquet("file.parquet")` |
| **Load JSON** | `df = manager.load_json("file.jsonl", json_format="lines")` |
| **Load from Snowflake** | `df = manager.load_from_snowflake(query="...", connection_params={...})` |
| **Save Parquet** | `manager.save_parquet(df, "output.parquet", compression="zstd")` |
| **Save JSON** | `manager.save_json(df, "output.jsonl", json_format="lines")` |
| **Upload to Snowflake** | `manager.upload_to_snowflake(df, "table_name", connection_params={...})` |

## 🔍 Matching Algorithms

| Method | Use Case | Example |
|--------|----------|---------|
| **jaro_winkler** | Names, companies | `method="jaro_winkler", threshold=0.85` |
| **levenshtein** | Typos, short strings | `method="levenshtein", threshold=0.8` |
| **token_set_ratio** | Addresses, word order | `method="token_set_ratio", threshold=0.7` |
| **cosine** | Long text, descriptions | `method="cosine", threshold=0.6` |
| **exact** | Exact matching | `manager.exact_match(...)` |

## 🎯 Fuzzy Matching

### Basic Fuzzy Match

```python
matches = manager.fuzzy_match(
    source_df=customers,
    target_df=vendors,
    source_col="company_name",
    target_col="vendor_name",
    method="jaro_winkler",
    threshold=0.85,
    top_k=1  # Best match only
)

for match in matches:
    print(f"{match.source_value} → {match.target_value} ({match.score:.3f})")
```

### Return as DataFrame

```python
matches_df = manager.fuzzy_match_polars(
    source_df=customers,
    target_df=vendors,
    source_col="name",
    target_col="name",
    method="jaro_winkler",
    threshold=0.85
)
```

## 🔧 DuckDB SQL Queries

```python
# Register DataFrame
manager.register_dataframe(df, "my_table")

# Register Parquet (zero-copy)
manager.register_parquet("data.parquet", "large_table")

# Query with SQL
result = manager.query("""
    SELECT 
        customer_name,
        COUNT(*) as order_count
    FROM my_table
    GROUP BY customer_name
    ORDER BY order_count DESC
""")
```

## 🔄 Deduplication

```python
# Exact deduplication
deduped = manager.deduplicate(
    df=products,
    columns=["sku"],
    method="exact"
)

# Fuzzy deduplication
deduped = manager.deduplicate(
    df=products,
    columns=["product_name"],
    method="fuzzy",
    fuzzy_threshold=0.9
)
```

## ⚡ Performance Tips

| Tip | Code |
|-----|------|
| **Lazy loading** | `df = manager.load_parquet("file.parquet", lazy=True)` |
| **Batch processing** | `fuzzy_match(..., batch_size=10_000)` |
| **N-gram index** | `index = create_fuzzy_index(df, "name", ngram_size=3)` |
| **Compression** | `save_parquet(df, "out.parquet", compression="zstd")` |

## 🎬 Common Patterns

### Pattern 1: Load → Match → Save

```python
with DuckDBDataManager(auto_configure=True) as manager:
    # Load
    source = manager.load_parquet("customers.parquet")
    target = manager.load_json("vendors.jsonl")
    
    # Match
    matches = manager.fuzzy_match_polars(
        source_df=source,
        target_df=target,
        source_col="name",
        target_col="name",
        method="jaro_winkler",
        threshold=0.85
    )
    
    # Save
    manager.save_parquet(matches, "matched.parquet")
```

### Pattern 2: Snowflake → Process → Snowflake

```python
with DuckDBDataManager(auto_configure=True) as manager:
    # Load from Snowflake
    df = manager.load_from_snowflake(
        query="SELECT * FROM raw_data",
        connection_params=sf_params
    )
    
    # Process
    cleaned = manager.deduplicate(df, columns=["id"], method="fuzzy")
    
    # Upload back
    manager.upload_to_snowflake(
        df=cleaned,
        table_name="cleaned_data",
        connection_params=sf_params,
        if_exists="replace"
    )
```

### Pattern 3: Large-Scale Matching with Index

```python
from fleet_q.duckdb_utils import create_fuzzy_index, search_fuzzy_index

with DuckDBDataManager(auto_configure=True) as manager:
    # Create index on target (one-time cost)
    index = create_fuzzy_index(
        df=large_target,
        column="company_name",
        ngram_size=3
    )
    
    # For each source record
    for source_row in source_df.iter_rows(named=True):
        # Find candidates (fast)
        candidates = search_fuzzy_index(
            query=source_row["name"],
            index=index,
            min_overlap=3
        )
        
        # Precise match on candidates only
        if candidates:
            target_subset = large_target[candidates]
            matches = manager.fuzzy_match(...)
```

## 📖 Algorithm Recommendations

| Data Type | Recommended Method | Threshold |
|-----------|-------------------|-----------|
| Person names | jaro_winkler | 0.85-0.90 |
| Company names | jaro_winkler | 0.80-0.85 |
| Addresses | token_set_ratio | 0.70-0.80 |
| Product names | token_sort_ratio | 0.75-0.85 |
| Descriptions | cosine | 0.60-0.70 |
| SKU/Codes | levenshtein | 0.90-0.95 |

## 🔗 Resources

- **[Complete Guide](../docs/DUCKDB_UTILS_GUIDE.md)** - Full documentation
- **[Examples](duckdb_matching_demo.py)** - Interactive demos
- **[API Reference](../docs/DUCKDB_UTILS_GUIDE.md#-api-reference)** - All methods

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Memory error | Use `lazy=True` or reduce `memory_limit` |
| Slow matching | Increase `batch_size` or use n-gram index |
| No Snowflake | Install: `pip install snowflake-connector-python` |
| No RapidFuzz | Install: `pip install rapidfuzz` |

## 💡 Best Practices

1. ✅ **Auto-configure** from pod resources
2. ✅ **Use context managers** for automatic cleanup
3. ✅ **Register Parquet views** instead of loading
4. ✅ **Index large targets** before matching
5. ✅ **Choose right algorithm** for your data type
6. ✅ **Batch large operations** (10K-50K rows)
7. ✅ **Compress with zstd** for Parquet files
8. ✅ **Test thresholds** on sample data first

---

**Ready to process data?** See the [Complete Guide](../docs/DUCKDB_UTILS_GUIDE.md)!
