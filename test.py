# -*- coding: utf-8 -*-
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    dbname="airflow",
    user="airflow",
    password="airflow",
    port=5432
)

cur = conn.cursor()
cur.execute("SELECT * FROM dw.dw_ability LIMIT 10")
rows = cur.fetchall()

for row in rows:
    print(row)

cur.close()
conn.close()