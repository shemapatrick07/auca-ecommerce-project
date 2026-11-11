"""
PART 2 - Spark batch processing (PySpark DataFrames)

Step 1  CLEAN & NORMALISE the raw JSON  -> output/clean/*.parquet
Step 2  PRODUCT RECOMMENDATION INDICATORS
          a) "customers who bought X also bought Y"   (co-purchase: support / confidence / lift)
          b) "products frequently viewed together"    (co-view from sessions)
Step 3  COHORT ANALYSIS  (registration-month cohorts -> spend in later months)

Run:  python scripts/06_spark_batch.py        (or: spark-submit scripts/06_spark_batch.py)
"""
from pyspark.sql import functions as F, Window

import config
from spark_common import (get_spark, read_raw, clean_path, save_csv, USERS_SCHEMA, CATEGORIES_SCHEMA,
                          PRODUCTS_SCHEMA, TRANSACTIONS_SCHEMA, SESSIONS_SCHEMA)

MIN_SUPPORT = 2          # minimum number of co-occurrences kept for pairs

spark = get_spark("06-batch")
spark.sparkContext.setLogLevel("WARN")

# =============================================================================================
# Step 1 - cleaning
# =============================================================================================
print("\n[1/3] Reading raw JSON with explicit schemas ...")
raw_users = read_raw(spark, "users.json", USERS_SCHEMA)
raw_cats = read_raw(spark, "categories.json", CATEGORIES_SCHEMA)
raw_prod = read_raw(spark, "products.json", PRODUCTS_SCHEMA)
raw_tx = read_raw(spark, "transactions.json", TRANSACTIONS_SCHEMA)
raw_sess = read_raw(spark, "sessions_*.json", SESSIONS_SCHEMA)

# ---- users: dedupe, parse timestamps, standardise geo strings, drop impossible rows
users = (raw_users.dropDuplicates(["user_id"])
         .filter(F.col("user_id").isNotNull())
         .withColumn("registration_date", F.col("registration_date").cast("timestamp"))
         .withColumn("last_active", F.col("last_active").cast("timestamp"))
         .withColumn("country", F.upper(F.trim("geo_data.country")))
         .withColumn("state", F.upper(F.trim("geo_data.state")))
         .withColumn("city", F.trim("geo_data.city"))
         .drop("geo_data")
         .filter(F.col("registration_date").isNotNull())
         .filter(F.col("last_active") >= F.col("registration_date")))

# ---- categories: dedupe + average sub-category margin
cats = (raw_cats.dropDuplicates(["category_id"])
        .withColumn("avg_margin", F.expr("aggregate(subcategories, 0D, (a, x) -> a + x.profit_margin) "
                                         "/ size(subcategories)")))

# ---- products: positive price, missing stock -> 0, missing flag -> inactive, typed price history
products = (raw_prod.dropDuplicates(["product_id"])
            .filter(F.col("product_id").isNotNull() & (F.col("base_price") > 0))
            .withColumn("current_stock", F.coalesce("current_stock", F.lit(0)))
            .withColumn("is_active", F.coalesce("is_active", F.lit(False)))
            .withColumn("creation_date", F.col("creation_date").cast("timestamp"))
            .withColumn("price_history", F.expr("transform(price_history, p -> named_struct("
                                                "'price', p.price, 'date', cast(p.date as timestamp)))"))
            .withColumn("initial_price", F.col("price_history")[0]["price"])
            .withColumn("price_changes", F.size("price_history") - 1))

# ---- transactions: dedupe, typed ts, normalised strings, valid items, consistency flag
tx = (raw_tx.dropDuplicates(["transaction_id"])
      .filter(F.col("user_id").isNotNull() & (F.size("items") > 0) & (F.col("total") >= 0))
      .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
      .withColumn("status", F.lower(F.trim("status")))
      .withColumn("payment_method", F.lower(F.trim("payment_method")))
      .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
      .withColumn("has_session", F.col("session_id").isNotNull())
      .withColumn("items_total", F.expr("aggregate(items, 0D, (a, x) -> a + x.subtotal)"))
      .withColumn("is_consistent", F.abs(F.col("items_total") - F.col("subtotal")) < 0.02)
      .filter(F.col("timestamp").isNotNull()))

# ---- sessions: dedupe, typed ts, flatten device/geo, cart map -> array, normalise categoricals
sessions = (raw_sess.dropDuplicates(["session_id"])
            .filter(F.col("user_id").isNotNull() & (F.col("duration_seconds") > 0))
            .withColumn("start_time", F.col("start_time").cast("timestamp"))
            .withColumn("end_time", F.col("end_time").cast("timestamp"))
            .withColumn("device_type", F.lower("device_profile.type"))
            .withColumn("os", F.col("device_profile.os"))
            .withColumn("browser", F.col("device_profile.browser"))
            .withColumn("country", F.upper("geo_data.country"))
            .withColumn("referrer", F.lower(F.coalesce("referrer", F.lit("unknown"))))
            .withColumn("page_views", F.expr(
                "transform(page_views, pv -> named_struct('timestamp', cast(pv.timestamp as timestamp), "
                "'page_type', pv.page_type, 'product_id', pv.product_id, "
                "'category_id', pv.category_id, 'view_duration', pv.view_duration))"))
            .withColumn("cart_items", F.expr(
                "transform(map_keys(cart_contents), k -> named_struct('product_id', k, "
                "'quantity', cart_contents[k].quantity, 'price', cart_contents[k].price))"))
            .drop("device_profile", "geo_data", "cart_contents"))

for name, df in [("users", users), ("categories", cats), ("products", products),
                 ("transactions", tx), ("sessions", sessions)]:
    df.write.mode("overwrite").parquet(clean_path(name))

# cleaning report (reads the parquet we just wrote -> no recomputation of the raw pipeline)
print("\nCleaning report (raw rows -> clean rows)")
for name, raw in [("users", raw_users), ("categories", raw_cats), ("products", raw_prod),
                  ("transactions", raw_tx), ("sessions", raw_sess)]:
    clean_n = spark.read.parquet(clean_path(name)).count()
    print(f"  {name:13s} {raw.count():>10,} -> {clean_n:>10,}")

users, products, tx, sessions = (spark.read.parquet(clean_path(n))
                                 for n in ["users", "products", "transactions", "sessions"])
n_tx = tx.count()
inconsistent = tx.filter(~F.col("is_consistent")).count()
print(f"  transactions whose line items do not add up to subtotal: {inconsistent:,}")

# =============================================================================================
# Step 2a - co-purchase ("customers who bought X also bought Y")
# =============================================================================================
print("\n[2/3] Recommendation indicators ...")
items = (tx.select("transaction_id", F.explode("items").alias("it"))
           .select("transaction_id", F.col("it.product_id").alias("pid")).distinct())
prod_counts = items.groupBy("pid").agg(F.count("*").alias("n"))

a, b = items.alias("a"), items.alias("b")
pairs = (a.join(b, (F.col("a.transaction_id") == F.col("b.transaction_id")) & (F.col("a.pid") < F.col("b.pid")))
          .groupBy(F.col("a.pid").alias("p1"), F.col("b.pid").alias("p2"))
          .agg(F.count("*").alias("pair_count"))
          .filter(F.col("pair_count") >= MIN_SUPPORT))
directional = (pairs.select(F.col("p1").alias("antecedent"), F.col("p2").alias("consequent"), "pair_count")
               .union(pairs.select(F.col("p2"), F.col("p1"), "pair_count")))
names = products.select(F.col("product_id").alias("pid"), "name")
co_purchase = (directional
               .join(prod_counts.withColumnRenamed("pid", "antecedent").withColumnRenamed("n", "n_ante"), "antecedent")
               .join(prod_counts.withColumnRenamed("pid", "consequent").withColumnRenamed("n", "n_cons"), "consequent")
               .withColumn("support", F.col("pair_count") / F.lit(n_tx))
               .withColumn("confidence", F.col("pair_count") / F.col("n_ante"))
               .withColumn("lift", F.col("confidence") / (F.col("n_cons") / F.lit(n_tx)))
               .join(names.withColumnRenamed("pid", "antecedent").withColumnRenamed("name", "antecedent_name"), "antecedent")
               .join(names.withColumnRenamed("pid", "consequent").withColumnRenamed("name", "consequent_name"), "consequent")
               .select("antecedent", "antecedent_name", "consequent", "consequent_name",
                       "pair_count", "support", "confidence", "lift"))
co_purchase.write.mode("overwrite").parquet(clean_path("co_purchase"))
print("  Top co-purchase rules:")
top_cp = co_purchase.orderBy(F.desc("pair_count"), F.desc("lift")).limit(50)
top_cp.show(10, truncate=40)
save_csv(top_cp, "spark_copurchase.csv")

# =============================================================================================
# Step 2b - co-view (products viewed together in the same session)
# =============================================================================================
views = (sessions.select("session_id", F.explode("viewed_products").alias("pid")).distinct())
va, vb = views.alias("a"), views.alias("b")
co_view = (va.join(vb, (F.col("a.session_id") == F.col("b.session_id")) & (F.col("a.pid") < F.col("b.pid")))
             .groupBy(F.col("a.pid").alias("product_a"), F.col("b.pid").alias("product_b"))
             .agg(F.count("*").alias("sessions_together"))
             .filter(F.col("sessions_together") >= MIN_SUPPORT))
top_cv = co_view.orderBy(F.desc("sessions_together")).limit(50)
print("  Products most frequently viewed together:")
top_cv.show(10)
save_csv(top_cv, "spark_coview.csv")

# =============================================================================================
# Step 3 - cohort analysis
# =============================================================================================
print("\n[3/3] Cohort analysis ...")
u = users.select("user_id", F.trunc("registration_date", "month").alias("cohort_month"))
cohort_size = u.groupBy("cohort_month").agg(F.count("*").alias("cohort_users"))
tx_u = (tx.join(u, "user_id")
          .withColumn("txn_month", F.trunc("timestamp", "month"))
          .withColumn("months_since_registration",
                      F.months_between("txn_month", "cohort_month").cast("int")))
cohorts = (tx_u.groupBy("cohort_month", "months_since_registration")
           .agg(F.countDistinct("user_id").alias("buyers"), F.count("*").alias("orders"),
                F.round(F.sum("total"), 2).alias("revenue"))
           .join(cohort_size, "cohort_month")
           .withColumn("avg_spend_per_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
           .withColumn("buyer_rate", F.round(F.col("buyers") / F.col("cohort_users"), 4))
           .orderBy("cohort_month", "months_since_registration"))
cohorts.show(20)
save_csv(cohorts, "spark_cohorts.csv")
print("\nDone. Clean data in", config.OUT_DIR + "/clean")
spark.stop()
