"""Shared configuration. Override any value with an environment variable."""
import os

DATA_DIR = os.environ.get("DATA_DIR", "data")        # where generated JSON files live
OUT_DIR = os.environ.get("OUT_DIR", "output")        # CSVs, parquet, figures
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

os.makedirs(OUT_DIR, exist_ok=True)
