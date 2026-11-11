"""Spark helpers: session factory, explicit schemas, readers/writers."""
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
import config

CLEAN_DIR = os.path.join(config.OUT_DIR, "clean")


def get_spark(name="ecommerce-analytics"):
    return (SparkSession.builder.appName(name)
            .master(os.environ.get("SPARK_MASTER", "local[*]"))
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "32"))
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


S, D, L = T.StringType(), T.DoubleType(), T.LongType()
B = T.BooleanType()


def _st(*fields):
    return T.StructType([T.StructField(n, t, True) for n, t in fields])


GEO = _st(("city", S), ("state", S), ("country", S))
USERS_SCHEMA = _st(("user_id", S), ("geo_data", GEO), ("registration_date", S), ("last_active", S))
CATEGORIES_SCHEMA = _st(
    ("category_id", S), ("name", S),
    ("subcategories", T.ArrayType(_st(("subcategory_id", S), ("name", S), ("profit_margin", D)))))
PRODUCTS_SCHEMA = _st(
    ("product_id", S), ("name", S), ("category_id", S), ("subcategory_id", S),
    ("base_price", D), ("current_stock", L), ("is_active", B),
    ("price_history", T.ArrayType(_st(("price", D), ("date", S)))), ("creation_date", S))
TRANSACTIONS_SCHEMA = _st(
    ("transaction_id", S), ("session_id", S), ("user_id", S), ("timestamp", S),
    ("items", T.ArrayType(_st(("product_id", S), ("quantity", L), ("unit_price", D), ("subtotal", D)))),
    ("subtotal", D), ("discount", D), ("total", D), ("payment_method", S), ("status", S))
# cart_contents has DYNAMIC keys (product ids) -> must be a MapType, otherwise schema inference
# would create one struct field per product id (thousands of columns).
SESSIONS_SCHEMA = _st(
    ("session_id", S), ("user_id", S), ("start_time", S), ("end_time", S), ("duration_seconds", L),
    ("geo_data", _st(("city", S), ("state", S), ("country", S), ("ip_address", S))),
    ("device_profile", _st(("type", S), ("os", S), ("browser", S))),
    ("viewed_products", T.ArrayType(S)),
    ("page_views", T.ArrayType(_st(("timestamp", S), ("page_type", S), ("product_id", S),
                                   ("category_id", S), ("view_duration", L)))),
    ("cart_contents", T.MapType(S, _st(("quantity", L), ("price", D)))),
    ("conversion_status", S), ("referrer", S))


def read_raw(spark, pattern, schema):
    return (spark.read.schema(schema).option("multiLine", True)
            .json(os.path.join(config.DATA_DIR, pattern)))


def clean_path(name):
    return os.path.join(CLEAN_DIR, name)


def load_clean(spark, name):
    p = clean_path(name)
    if not os.path.exists(p):
        raise SystemExit(f"{p} not found - run scripts/06_spark_batch.py first.")
    return spark.read.parquet(p)


def save_csv(df, name):
    """Save a SMALL aggregated Spark DataFrame as a single CSV (for the visualisation step)."""
    path = os.path.join(config.OUT_DIR, name)
    df.toPandas().to_csv(path, index=False)
    print(f"  -> saved {path}")
