"""
Data verification script for Milvus and PostgreSQL consistency checking.

This script verifies data consistency between Milvus collections and their 
corresponding PostgreSQL storage by comparing entities.
"""

import os
import argparse

from dotenv import load_dotenv

from pymilvus_pg import MilvusPGClient as MilvusClient

# Load environment variables from .env file
load_dotenv()


def main():
    """
    Main function to verify data consistency between Milvus and PostgreSQL.
    
    This function parses command line arguments, creates a MilvusPGClient,
    and performs entity comparison for all collections matching the specified prefix.
    """
    parser = argparse.ArgumentParser(description="Verify Milvus and PostgreSQL consistency")
    parser.add_argument("--uri", type=str, default=os.getenv("MILVUS_URI", "http://localhost:19530"), help="Milvus server URI")
    parser.add_argument("--pg_conn", type=str, default=os.getenv("PG_CONN", "postgresql://postgres:admin@localhost:5432/default"), help="PostgreSQL DSN")
    parser.add_argument("--collection_name_prefix", type=str, default="data_correctness_checker", help="Collection name prefix")
    parser.add_argument("--batch_size", type=int, default=10000, help="Batch size for entity comparison (default: 10000)")
    parser.add_argument("--full_scan", action="store_true", help="Enable full scan mode for entity comparison")
    args = parser.parse_args()
    
    # Initialize Milvus client with PostgreSQL connection
    milvus_client = MilvusClient(uri=args.uri, pg_conn_str=args.pg_conn)
    
    # Get all collections and filter by prefix
    collections = milvus_client.list_collections()
    for collection in collections:
        if collection.startswith(args.collection_name_prefix):
            # Perform entity comparison with configurable parameters
            milvus_client.entity_compare(collection, batch_size=args.batch_size, full_scan=args.full_scan)


if __name__ == "__main__":
    main()