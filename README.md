# E-commerce Analytics: MongoDB, HBase and Spark
AUCA Big Data Analytics Final Project - Shema Patrick (101375)

## How to reproduce
1. pip install -r requirements.txt
2. python scripts/generate_dataset.py   (creates the data/ folder)
3. docker compose up -d   (starts MongoDB and HBase)
4. Run the scripts in scripts/ in order: 01-02 MongoDB, 03-05 HBase, 06-07 Spark, 08 integrated analytics, 09 charts
