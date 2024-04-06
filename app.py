#Author: Joshua Omolewa

#importing relevant libraries an modules for project
import os
import logging 
import requests
import time
import psycopg2
from dotenv import load_dotenv
import pandas as pd 

# Load the environment variables 
load_dotenv()

# def connect_to_db():
#     conn = psycopg2.connect(
#         host=os.environ["DATABASE_HOST"],
#         dbname=os.environ["DATABASE_NAME"],
#         user=os.environ["DATABASE_USER"],
#         password=os.environ["DATABASE_PASSWORD"],
#     )
#     cur = conn.cursor()
#     cur.execute("SELECT * FROM information_schema.tables;")
#     rows = cur.fetchall()
#     for row in rows:
#         print(row)
#     cur.close()
#     conn.close()

extract_set = ["add-cover", "add-book", "edit-book", "merge-authors"]


# url = "http://openlibrary.org/recentchanges/2024/01/01/merge-authors.json"

params = {"limit": 1}

# response = requests.get(url, params=params)

def extract_data(specific_date, kind, params):

    url = f"http://openlibrary.org/recentchanges/{specific_date}/{kind}.json"
    response = requests.get(url, params=params)
    return response.json()
    



# print(response.json())

#generate dates for data extraction using pandas
def generate_date(start_date, end_date):
    """
    This function generate dates based on 
    start date and end date 

    start_date(string) in YYYY-MM-DD
    end_date(string) in YYYY-MM-DD

    return a pandas datetimeindex
    """
    date_range = pd.date_range(start=start_date, end=end_date)
    return date_range


if __name__ == "__main__":

    dates = generate_date("2024-01-01","2024-01-03")


    for date1 in dates:
        a = date1.strftime("%Y/%m/%d")
        # print(a)
        for set1 in extract_set:
            print(extract_data(a, set1, params))
        





# if __name__ == "__main__":
#     print("Data Engineering Take Home Assessment")
#     connect_to_db()
