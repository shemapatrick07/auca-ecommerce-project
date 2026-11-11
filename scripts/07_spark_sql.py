"""
PART 2 - Spark SQL analytics on the cleaned DataFrames (run 06_spark_batch.py first).

Q1  revenue, margin and estimated profit per category         (CTE, LATERAL VIEW explode, joins)
Q2  daily revenue, 7-day moving average, day-over-day growth  (window functions)
Q3  conversion rate by referrer x device                      (conditional aggregation)
Q4  most-viewed products vs units sold                        (two CTEs + join)
Q5  CROSS-STORE query: "MongoDB" user profile  JOIN  "HBase" engagement metrics (NTILE engagement tiers)
    -> the two views below stand in for data that would live in MongoDB / HBase. In the integrated
       pipeline (08) the same logic runs on data pulled from the real databases.
"""
from spark_common import get_spark, load_clean, save_csv

spark = get_spark("07-sql")
spark.sparkContext.setLogLevel("WARN")

for name in ["users", "categories", "products", "transactions", "sessions"]:
    load_clean(spark, name).createOrReplaceTempView(name)

# ---- views that stand in for the two NoSQL stores ------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW mongo_user_profile AS      -- users + purchase_summary (MongoDB users collection)
    SELECT u.user_id, u.country, u.registration_date,
           COUNT(t.transaction_id)         AS orders,
           COALESCE(SUM(t.total), 0.0)     AS total_spent
    FROM users u LEFT JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.country, u.registration_date
""")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW hbase_user_engagement AS   -- per-user prefix scan of HBase user_sessions
    SELECT user_id, COUNT(*) AS session_count, ROUND(AVG(duration_seconds), 1) AS avg_duration,
           MAX(start_time) AS last_session
    FROM sessions GROUP BY user_id
""")

QUERIES = {
"Q1 revenue / margin by category": """
    WITH items AS (
        SELECT t.transaction_id, x.it.product_id AS product_id, x.it.quantity AS quantity, x.it.subtotal AS subtotal
        FROM transactions t LATERAL VIEW explode(t.items) x AS it
    )
    SELECT c.category_id, c.name AS category,
           COUNT(DISTINCT i.transaction_id)      AS orders,
           SUM(i.quantity)                       AS units,
           ROUND(SUM(i.subtotal), 2)             AS revenue,
           ROUND(c.avg_margin, 3)                AS avg_margin,
           ROUND(SUM(i.subtotal) * c.avg_margin, 2) AS est_profit
    FROM items i
    JOIN products   p ON i.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    GROUP BY c.category_id, c.name, c.avg_margin
    ORDER BY revenue DESC
""",
"Q2 daily revenue, moving average, growth": """
    WITH daily AS (
        SELECT CAST(timestamp AS DATE) AS day, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
        FROM transactions GROUP BY CAST(timestamp AS DATE)
    )
    SELECT day, orders, revenue,
           ROUND(AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_avg,
           ROUND((revenue - LAG(revenue) OVER (ORDER BY day)) / LAG(revenue) OVER (ORDER BY day) * 100, 2) AS dod_growth_pct
    FROM daily ORDER BY day
""",
"Q3 conversion by referrer and device": """
    SELECT referrer, device_type,
           COUNT(*) AS sessions,
           SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS conversions,
           ROUND(100 * AVG(CASE WHEN conversion_status = 'converted' THEN 1.0 ELSE 0.0 END), 3) AS conv_rate_pct,
           ROUND(AVG(duration_seconds), 1) AS avg_duration_s
    FROM sessions GROUP BY referrer, device_type ORDER BY conv_rate_pct DESC
""",
"Q4 most viewed vs units sold": """
    WITH v AS (SELECT pid, COUNT(*) AS views FROM (SELECT explode(viewed_products) AS pid FROM sessions) GROUP BY pid),
         s AS (SELECT x.it.product_id AS pid, SUM(x.it.quantity) AS units
               FROM transactions t LATERAL VIEW explode(t.items) x AS it GROUP BY x.it.product_id)
    SELECT v.pid AS product_id, p.name, v.views, COALESCE(s.units, 0) AS units_sold,
           ROUND(COALESCE(s.units, 0) / v.views, 4) AS units_per_view
    FROM v LEFT JOIN s ON v.pid = s.pid JOIN products p ON v.pid = p.product_id
    ORDER BY v.views DESC LIMIT 15
""",
"Q5 cross-store: engagement tier vs spend": """
    WITH joined AS (
        SELECT m.user_id, m.country, m.orders, m.total_spent, e.session_count, e.avg_duration
        FROM mongo_user_profile m JOIN hbase_user_engagement e ON m.user_id = e.user_id
    ), tiered AS (
        SELECT *, NTILE(4) OVER (ORDER BY session_count) AS engagement_tier FROM joined
    )
    SELECT engagement_tier, COUNT(*) AS users,
           ROUND(AVG(session_count), 1) AS avg_sessions,
           ROUND(AVG(orders), 2)        AS avg_orders,
           ROUND(AVG(total_spent), 2)   AS avg_spent
    FROM tiered GROUP BY engagement_tier ORDER BY engagement_tier
""",
}

for title, sql in QUERIES.items():
    print(f"\n===== {title} =====")
    df = spark.sql(sql)
    df.show(15, truncate=40)
    if title.startswith("Q1"):
        save_csv(df, "spark_sql_category_margin.csv")
    elif title.startswith("Q2"):
        save_csv(df, "spark_daily_revenue.csv")
    elif title.startswith("Q3"):
        save_csv(df, "spark_sql_conversion.csv")
    elif title.startswith("Q5"):
        save_csv(df, "spark_sql_engagement_tiers.csv")

spark.stop()
