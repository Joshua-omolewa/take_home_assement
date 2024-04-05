import os
import requests
import time
import psycopg2


def connect_to_db():
    conn = psycopg2.connect(
        host=os.environ["DATABASE_HOST"],
        dbname=os.environ["DATABASE_NAME"],
        user=os.environ["DATABASE_USER"],
        password=os.environ["DATABASE_PASSWORD"],
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM information_schema.tables;")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    cur.close()
    conn.close()


if __name__ == "__main__":
    print("Data Engineering Take Home Assessment")
    connect_to_db()
