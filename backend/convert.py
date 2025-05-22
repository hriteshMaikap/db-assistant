import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from dotenv import load_dotenv

load_dotenv()

# Config
SQLITE_PATH = "C:\\Users\\hrite\\OneDrive\\Documents\\db-assistant\\backend\\ecommerce_test.db"
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "hritesh12345")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
TARGET_DB   = "ecommerce_test"

# Engines
src_engine = create_engine(f"sqlite:///{SQLITE_PATH}")
admin_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/"
)
tgt_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_DB}"
)

# 1. Create target DB
with admin_engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}"))
    conn.commit()

# 2. Reflect SQLite schema
src_meta = MetaData()
src_meta.reflect(bind=src_engine)

# 3. Recreate schema in MySQL
# Fix: Create MetaData without 'bind' parameter
tgt_meta = MetaData()
import sqlalchemy as sa
for tbl in src_meta.sorted_tables:
    cols = [c.copy() for c in tbl.columns]
    sa.Table(tbl.name, tgt_meta, *cols)
# Use the engine when creating tables, not in the MetaData constructor
tgt_meta.create_all(tgt_engine)

# 4. Transfer data via pandas
for table_name in src_meta.tables.keys():
    df = pd.read_sql_table(table_name, src_engine)
    if not df.empty:
        df.to_sql(table_name,
                  tgt_engine,
                  if_exists="append",
                  index=False,
                  chunksize=500)
    print(f"Transferred {len(df)} rows into `{table_name}`")

print("Migration complete!")