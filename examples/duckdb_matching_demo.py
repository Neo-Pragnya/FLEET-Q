"""
DuckDB Fuzzy Matching Demo

Demonstrates real-world data matching scenarios using DuckDB utilities:
1. Customer-Vendor reconciliation
2. Product deduplication
3. Address matching
4. Large-scale indexed matching

Requirements:
    pip install duckdb polars rapidfuzz

Usage:
    python examples/duckdb_matching_demo.py
"""

import sys
from pathlib import Path

# Add parent directory to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from fleet_q.duckdb_utils import (
    DuckDBDataManager,
    create_fuzzy_index,
    search_fuzzy_index
)


def demo_customer_vendor_matching():
    """Demo 1: Customer-Vendor Reconciliation"""
    print("="*80)
    print("DEMO 1: Customer-Vendor Reconciliation")
    print("="*80)
    
    # Sample customer data
    customers = pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": [
            "Apple Inc",
            "Microsoft Corporation",
            "Google LLC",
            "Amazon.com",
            "Meta Platforms"
        ],
        "region": ["US", "US", "US", "US", "US"]
    })
    
    # Sample vendor data (with typos and variations)
    vendors = pl.DataFrame({
        "vendor_id": [101, 102, 103, 104, 105, 106],
        "vendor_name": [
            "Apple Incorporated",
            "Microsft Corp",  # Typo
            "Google",
            "Amazon",
            "Facebook Inc",  # Old name for Meta
            "Tesla Motors"
        ],
        "vendor_code": ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA"]
    })
    
    print(f"\n📊 Dataset sizes:")
    print(f"   Customers: {len(customers)}")
    print(f"   Vendors: {len(vendors)}")
    
    # Initialize manager
    with DuckDBDataManager(auto_configure=True) as manager:
        print("\n🔍 Running fuzzy matching (Jaro-Winkler, threshold=0.8)...")
        
        matches = manager.fuzzy_match(
            source_df=customers,
            target_df=vendors,
            source_col="customer_name",
            target_col="vendor_name",
            source_id="customer_id",
            target_id="vendor_id",
            method="jaro_winkler",
            threshold=0.8,
            top_k=1
        )
        
        print(f"\n✅ Found {len(matches)} matches:\n")
        for match in matches:
            confidence = "HIGH" if match.score > 0.9 else "MEDIUM"
            print(f"   [{confidence}] {match.source_value}")
            print(f"            → {match.target_value}")
            print(f"            Score: {match.score:.3f}")
            print()


def demo_product_deduplication():
    """Demo 2: Product Catalog Deduplication"""
    print("="*80)
    print("DEMO 2: Product Catalog Deduplication")
    print("="*80)
    
    # Sample products with duplicates and near-duplicates
    products = pl.DataFrame({
        "product_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "sku": ["SKU001", "SKU001", "SKU002", "SKU003", "SKU003", "SKU004", "SKU005", "SKU006"],
        "product_name": [
            "iPhone 15 Pro Max",
            "iPhone 15 Pro Max",  # Exact duplicate
            "Samsung Galaxy S24 Ultra",
            "MacBook Pro 16-inch",
            "MacBook Pro 16 inch",  # Fuzzy duplicate (hyphen vs space)
            "Dell XPS 15",
            "Dell XPS 15 Laptop",  # Fuzzy duplicate (with "Laptop")
            "Sony WH-1000XM5"
        ],
        "price": [1199.99, 1199.99, 1299.99, 2499.99, 2499.99, 1799.99, 1799.99, 399.99]
    })
    
    print(f"\n📊 Original product count: {len(products)}")
    
    with DuckDBDataManager(auto_configure=True) as manager:
        # Step 1: Exact deduplication by SKU
        print("\n🔍 Step 1: Removing exact duplicates by SKU...")
        exact_deduped = manager.deduplicate(
            df=products,
            columns=["sku"],
            method="exact"
        )
        print(f"   Removed {len(products) - len(exact_deduped)} exact duplicates")
        print(f"   Remaining: {len(exact_deduped)} products")
        
        # Step 2: Fuzzy deduplication by product name
        print("\n🔍 Step 2: Removing fuzzy duplicates by product name...")
        fuzzy_deduped = manager.deduplicate(
            df=exact_deduped,
            columns=["product_name"],
            method="fuzzy",
            fuzzy_threshold=0.9
        )
        print(f"   Removed {len(exact_deduped) - len(fuzzy_deduped)} fuzzy duplicates")
        print(f"   Final count: {len(fuzzy_deduped)} unique products")
        
        print("\n✅ Cleaned product catalog:")
        print(fuzzy_deduped.select(["sku", "product_name", "price"]))


def demo_address_matching():
    """Demo 3: Address Matching with Token-Based Algorithm"""
    print("\n" + "="*80)
    print("DEMO 3: Address Matching")
    print("="*80)
    
    # Addresses from System A
    system_a = pl.DataFrame({
        "id": [1, 2, 3],
        "address": [
            "123 Main Street, New York, NY 10001",
            "456 Oak Avenue, Los Angeles, CA 90001",
            "789 Pine Road, Chicago, IL 60601"
        ]
    })
    
    # Addresses from System B (different formatting)
    system_b = pl.DataFrame({
        "id": [101, 102, 103],
        "address": [
            "Main St 123, NY, New York 10001",  # Different word order
            "456 Oak Ave, LA, California 90001",  # Abbreviated
            "789 Pine Rd, Chicago Illinois 60601"  # No comma
        ]
    })
    
    print(f"\n📊 Addresses to match:")
    print(f"   System A: {len(system_a)}")
    print(f"   System B: {len(system_b)}")
    
    with DuckDBDataManager(auto_configure=True) as manager:
        print("\n🔍 Using token_set_ratio (handles word order differences)...")
        
        matches = manager.fuzzy_match(
            source_df=system_a,
            target_df=system_b,
            source_col="address",
            target_col="address",
            method="token_set_ratio",
            threshold=0.7,
            top_k=1
        )
        
        print(f"\n✅ Found {len(matches)} address matches:\n")
        for match in matches:
            print(f"   System A: {match.source_value}")
            print(f"   System B: {match.target_value}")
            print(f"   Score: {match.score:.3f}")
            print()


def demo_indexed_matching():
    """Demo 4: Large-Scale Matching with Indexing"""
    print("="*80)
    print("DEMO 4: Large-Scale Matching with N-Gram Index")
    print("="*80)
    
    # Simulate larger datasets
    import random
    
    # Generate source companies
    base_companies = [
        "Apple", "Microsoft", "Google", "Amazon", "Meta",
        "Tesla", "Netflix", "Adobe", "Salesforce", "Oracle",
        "Intel", "AMD", "Nvidia", "IBM", "Cisco"
    ]
    
    suffixes = ["Inc", "Corporation", "Corp", "LLC", "Ltd"]
    
    source_companies = []
    for i, base in enumerate(base_companies * 20):  # 300 companies
        suffix = random.choice(suffixes)
        source_companies.append({
            "id": i + 1,
            "company_name": f"{base} {suffix}"
        })
    
    source_df = pl.DataFrame(source_companies)
    
    # Generate target companies (with variations and typos)
    target_companies = []
    for i, base in enumerate(base_companies * 100):  # 1500 companies
        suffix = random.choice(suffixes)
        # Add some typos
        if random.random() < 0.1:
            base = base[:-1] + random.choice("abcdefghijklmnopqrstuvwxyz")
        target_companies.append({
            "id": i + 1,
            "vendor_name": f"{base} {suffix}"
        })
    
    target_df = pl.DataFrame(target_companies)
    
    print(f"\n📊 Dataset sizes:")
    print(f"   Source: {len(source_df):,} companies")
    print(f"   Target: {len(target_df):,} companies")
    print(f"   Naive comparison: {len(source_df) * len(target_df):,} comparisons")
    
    with DuckDBDataManager(auto_configure=True) as manager:
        # Create n-gram index
        print("\n🔍 Creating n-gram index on target dataset...")
        index = create_fuzzy_index(
            df=target_df,
            column="vendor_name",
            ngram_size=3
        )
        print(f"   Index created with {len(index):,} unique n-grams")
        
        # Search with index
        print("\n🔍 Searching with index (sample queries)...")
        sample_queries = source_df.head(5)["company_name"].to_list()
        
        total_candidates = 0
        for query in sample_queries:
            candidates = search_fuzzy_index(
                query=query,
                index=index,
                ngram_size=3,
                min_overlap=3
            )
            total_candidates += len(candidates)
            
            print(f"   Query: '{query}'")
            print(f"   → Found {len(candidates)} candidates (reduced from {len(target_df):,})")
        
        reduction = (1 - total_candidates / (len(sample_queries) * len(target_df))) * 100
        print(f"\n✅ Average reduction: {reduction:.1f}%")
        print(f"   Then apply precise fuzzy matching only on candidates")


def demo_comparison_table():
    """Demo 5: Algorithm Comparison"""
    print("\n" + "="*80)
    print("DEMO 5: Algorithm Comparison")
    print("="*80)
    
    # Test strings
    test_cases = [
        ("Apple Inc", "Apple Incorporated"),
        ("Microsoft Corporation", "Microsft Corp"),  # Typo
        ("Google LLC", "Google"),
        ("New York City", "City of New York"),  # Different word order
        ("123 Main St", "123 Main Street")
    ]
    
    methods = ["levenshtein", "jaro", "jaro_winkler", "token_set_ratio", "cosine"]
    
    print("\n📊 Comparing different matching algorithms:\n")
    print(f"{'Source':<25} {'Target':<25} {'Levenshtein':<12} {'Jaro':<8} {'JaroWinkler':<12} {'TokenSet':<10} {'Cosine':<8}")
    print("-" * 110)
    
    with DuckDBDataManager(auto_configure=True) as manager:
        for source, target in test_cases:
            scores = {}
            
            for method in methods:
                source_df = pl.DataFrame({"id": [1], "name": [source]})
                target_df = pl.DataFrame({"id": [1], "name": [target]})
                
                matches = manager.fuzzy_match(
                    source_df=source_df,
                    target_df=target_df,
                    source_col="name",
                    target_col="name",
                    method=method,
                    threshold=0.0,  # Get all scores
                    top_k=1
                )
                
                scores[method] = matches[0].score if matches else 0.0
            
            print(f"{source:<25} {target:<25} "
                  f"{scores['levenshtein']:>11.3f} "
                  f"{scores['jaro']:>7.3f} "
                  f"{scores['jaro_winkler']:>11.3f} "
                  f"{scores['token_set_ratio']:>9.3f} "
                  f"{scores['cosine']:>7.3f}")


def main():
    """Run all demos"""
    print("\n" + "="*80)
    print("DUCKDB FUZZY MATCHING DEMOS")
    print("="*80)
    print("\nThese demos show real-world data matching scenarios")
    print("using DuckDB, Polars, and RapidFuzz.\n")
    
    try:
        # Run demos
        demo_customer_vendor_matching()
        demo_product_deduplication()
        demo_address_matching()
        demo_indexed_matching()
        demo_comparison_table()
        
        print("\n" + "="*80)
        print("KEY TAKEAWAYS")
        print("="*80)
        print("""
✅ Jaro-Winkler works best for company names and person names
✅ Token-based methods handle different word orders (addresses)
✅ Levenshtein detects typos and small edits
✅ Cosine similarity works for longer text comparisons
✅ N-gram indexing speeds up large-scale matching by 90%+
✅ Combine exact + fuzzy deduplication for best results

📖 For more details, see docs/DUCKDB_UTILS_GUIDE.md
""")
    
    except ImportError as e:
        print(f"\n❌ Missing dependency: {e}")
        print("\nInstall required packages:")
        print("  pip install duckdb polars pyarrow rapidfuzz")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
