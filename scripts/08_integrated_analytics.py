"""
PART 3 - Integrated analytics across MongoDB + HBase + Spark

QUERY A - Customer Lifetime Value (CLV) estimation and customer segmentation
  Business question : "Which customers are worth the most over the next 12 months, and how do the
                       high-value customers behave differently?"
  Sources           : MongoDB  -> users (profile) + transactions (orders, revenue) + categories (margin)
                      HBase    -> user_sessions (session count, avg duration, conversions) via prefix-scan data
                                  (falls back to MongoDB `sessions` with --no-hbase)
  Spark             : joins the three extracts, computes CLV, scales features, runs K-Means (k=4)
  CLV formula       : CLV_12m = AOV x purchases_per_year x average_margin
                      AOV = revenue / orders ; purchases_per_year = orders / observation_days x 365

QUERY B - Funnel conversion analysis (Spark on the cleaned session data from 06_spark_batch.py)
  Business question : "Where do shoppers drop out between viewing a product and buying, per referrer?"
  Stages            : session -> viewed product -> added to cart -> reached checkout -> purchased

Usage: python scripts/08_integrated_analytics.py [--no-hbase] [--obs-days 90] [--skip-funnel]
"""
import argparse
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

import config
from spark_common import get_spark, load_clean, save_csv


# --------------------------------------------------------------------------- helpers ---
def to_spark(spark, pdf, name):
    """pandas -> Spark DataFrame through a temporary CSV file.
    (spark.createDataFrame(pandas_df) needs Python worker processes, which time out on some Windows
    machines; reading a file is handled entirely by the JVM.)"""
    path = os.path.join(config.OUT_DIR, f"tmp_{name}.csv")
    pdf.to_csv(path, index=False)
    return spark.read.csv(Path(path).resolve().as_uri(), header=True, inferSchema=True)


# ----------------------------------------------------------------------------- extraction ---
def extract_mongo(db):
    orders = pd.DataFrame(list(db.transactions.aggregate([
        {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "revenue": {"$sum": "$total"},
                    "last_purchase": {"$max": "$timestamp"}}},
        {"$project": {"_id": 0, "user_id": "$_id", "orders": 1, "revenue": 1, "last_purchase": 1}}],
        allowDiskUse=True)))
    users = pd.DataFrame(list(db.users.find(
        {}, {"_id": 0, "user_id": 1, "geo_data.country": 1, "registration_date": 1})))
    users["country"] = users["geo_data"].apply(lambda g: g.get("country") if isinstance(g, dict) else None)
    users = users[["user_id", "country", "registration_date"]]
    margin = list(db.categories.aggregate([
        {"$unwind": "$subcategories"},
        {"$group": {"_id": None, "m": {"$avg": "$subcategories.profit_margin"}}}]))
    return users, orders, float(margin[0]["m"]) if margin else 0.25


def extract_hbase():
    """Scan HBase user_sessions (only 2 small columns) and aggregate per user."""
    from hbase_common import connect
    table = connect().table("user_sessions")
    agg = defaultdict(lambda: [0, 0, 0])             # sessions, total seconds, converted
    for key, data in table.scan(columns=[b"info:duration_seconds", b"info:conversion_status"], batch_size=5000):
        a = agg[key.split(b"#", 1)[0].decode()]
        a[0] += 1
        a[1] += int(data.get(b"info:duration_seconds", b"0"))
        a[2] += data.get(b"info:conversion_status") == b"converted"
    return pd.DataFrame([{"user_id": u, "sessions": v[0], "avg_session_seconds": v[1] / v[0],
                          "converted_sessions": v[2]} for u, v in agg.items()])


def extract_sessions_from_mongo(db):
    return pd.DataFrame(list(db.sessions.aggregate([
        {"$group": {"_id": "$user_id", "sessions": {"$sum": 1}, "avg_session_seconds": {"$avg": "$duration_seconds"},
                    "converted_sessions": {"$sum": {"$cond": [{"$eq": ["$conversion_status", "converted"]}, 1, 0]}}}},
        {"$project": {"_id": 0, "user_id": "$_id", "sessions": 1, "avg_session_seconds": 1,
                      "converted_sessions": 1}}], allowDiskUse=True)))


# ------------------------------------------------------------------------------- query A -----
def clv_query(spark, args):
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    print("Extracting from MongoDB ...")
    users, orders, margin = extract_mongo(db)
    print(f"  users={len(users):,} buyers={len(orders):,} avg margin={margin:.3f}")

    if args.no_hbase:
        print("Extracting engagement from MongoDB sessions (--no-hbase) ...")
        eng = extract_sessions_from_mongo(db)
    else:
        print("Scanning HBase user_sessions ...")
        eng = extract_hbase()
    print(f"  engagement rows={len(eng):,}")

    # Spark does the join + computation
    u = to_spark(spark, users.assign(registration_date=users["registration_date"].astype(str)), "users")
    o = to_spark(spark, orders.assign(last_purchase=orders["last_purchase"].astype(str)), "orders")
    e = to_spark(spark, eng, "engagement")
    df = (u.join(o, "user_id", "left").join(e, "user_id", "left")
            .fillna({"orders": 0, "revenue": 0.0, "sessions": 0, "avg_session_seconds": 0.0, "converted_sessions": 0})
            .withColumn("aov", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")).otherwise(0.0))
            .withColumn("purchases_per_year", F.col("orders") / F.lit(args.obs_days) * 365)
            .withColumn("clv_12m", F.round(F.col("aov") * F.col("purchases_per_year") * F.lit(margin), 2))
            .withColumn("session_conv_rate", F.when(F.col("sessions") > 0,
                        F.col("converted_sessions") / F.col("sessions")).otherwise(0.0)))

    features = ["clv_12m", "sessions", "avg_session_seconds", "session_conv_rate"]
    vec = VectorAssembler(inputCols=features, outputCol="raw").transform(df)
    scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(vec).transform(vec)
    model = KMeans(k=4, seed=42, featuresCol="features").fit(scaled)
    out = model.transform(scaled).drop("raw", "features").toPandas()

    # name clusters by their average CLV (highest = Platinum)
    order = out.groupby("prediction")["clv_12m"].mean().sort_values(ascending=False).index.tolist()
    names = dict(zip(order, ["Platinum", "Gold", "Silver", "Bronze"]))
    out["segment"] = out["prediction"].map(names)
    out = out.drop(columns=["prediction"])
    out.to_csv(os.path.join(config.OUT_DIR, "integrated_clv.csv"), index=False)

    summary = (out.groupby("segment").agg(users=("user_id", "count"), avg_clv=("clv_12m", "mean"),
                                          avg_sessions=("sessions", "mean"), avg_orders=("orders", "mean"),
                                          avg_session_conv=("session_conv_rate", "mean"))
               .round(2).sort_values("avg_clv", ascending=False))
    summary.to_csv(os.path.join(config.OUT_DIR, "integrated_clv_segments.csv"))
    print("\nCLV segments (K-Means, k=4):")
    print(summary.to_string())
    print("  -> output/integrated_clv.csv, output/integrated_clv_segments.csv")


# ------------------------------------------------------------------------------- query B -----
def funnel_query(spark):
    s = load_clean(spark, "sessions")
    v = F.expr("exists(page_views, x -> x.page_type = 'product_detail')")
    chk = F.expr("exists(page_views, x -> x.page_type IN ('checkout', 'confirmation'))")
    flags = (s.select("referrer",
                      v.cast("int").alias("s1"),
                      (v & (F.size("cart_items") > 0)).cast("int").alias("s2"),
                      (v & (F.size("cart_items") > 0) & chk).cast("int").alias("s3"),
                      (v & (F.size("cart_items") > 0) & chk & (F.col("conversion_status") == "converted"))
                      .cast("int").alias("s4")))
    stages = ["viewed_product", "added_to_cart", "reached_checkout", "purchased"]
    sums = [F.sum(f"s{i + 1}").alias(n) for i, n in enumerate(stages)]
    overall = flags.agg(F.count("*").alias("all_sessions"), *sums).toPandas().iloc[0]
    seq = [("all_sessions", overall["all_sessions"])] + [(n, overall[n]) for n in stages]
    rows, prev = [], None
    for name, n in seq:
        rows.append({"stage": name, "sessions": int(n),
                     "pct_of_all": round(100 * n / seq[0][1], 2),
                     "step_conversion_pct": None if prev is None else round(100 * n / prev, 2)})
        prev = n
    funnel = pd.DataFrame(rows)
    funnel.to_csv(os.path.join(config.OUT_DIR, "integrated_funnel.csv"), index=False)
    print("\nFunnel (overall):")
    print(funnel.to_string(index=False))

    by_ref = (flags.groupBy("referrer").agg(F.count("*").alias("all_sessions"), *sums)
              .withColumn("view_to_purchase_pct", F.round(100 * F.col("purchased") / F.col("viewed_product"), 2))
              .orderBy(F.desc("view_to_purchase_pct")))
    print("\nFunnel by referrer:")
    by_ref.show()
    save_csv(by_ref, "integrated_funnel_by_referrer.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-hbase", action="store_true", help="take engagement metrics from MongoDB sessions")
    ap.add_argument("--obs-days", type=int, default=90, help="length of the observation window in days")
    ap.add_argument("--skip-funnel", action="store_true")
    args = ap.parse_args()
    spark = get_spark("08-integrated")
    spark.sparkContext.setLogLevel("WARN")
    clv_query(spark, args)
    if not args.skip_funnel:
        funnel_query(spark)
    spark.stop()
