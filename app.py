#Author: Joshua Omolewa

#importing relevant libraries an modules for project
import os
import logging 
import requests
import time
import psycopg2
from dotenv import load_dotenv
import pandas as pd 
from etl import * # importing all funciton from our ETL script

# configuring logging
logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

# Load the environment variables 
load_dotenv()


# #database connection configuration
# host=os.environ["DATABASE_HOST"],
# # dbname=os.environ["DATABASE_NAME"],
# user=os.environ["DATABASE_USER"],
# password=os.environ["DATABASE_PASSWORD"]





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

# project requirement 
extract_set = ["add-cover", "add-book", "edit-book", "merge-authors"]
api_start_date = "2023-12-01"
api_end_date = "2023-12-03"

# API restriction 
offset = 0
limit = 5


def extract_data(specific_date, kind, offset_value, limit_value):
    """
    This function returns the data extracted in a list format from the API
    based on specific date and using API restrictions including offset to
    paginate through response

    specific_date(str) : date in YYYY/MM/DD
    kind(str): book kind in in the set (add-cover, add-book, edit-book, merge-authors)
    offset_value(int): specifying API offset
    limit_value(int): specifying API limit

    return a list of data extracted
    """
    data_extracted = []
    offset = offset_value
    limit = limit_value
    try:
        while True:
            logging.info(f"Connecting to API to extract data from {specific_date} of kind {kind} and current offset is {offset}")

            # params = {"limit": limit , "offset":offset} # query string for API
            params = {"limit": limit } # query string for API

            url = f"http://openlibrary.org/recentchanges/{specific_date}/{kind}.json"
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise HTTPError for bad responses

            data = response.json()

            data_extracted.extend(data)

            break

            # # checking if data returned is empty as API returns a list so I can stop data extraction based on offset
            # checkpoint = data if isinstance(data, list) else [data]

            
            # if not checkpoint:
            #     #Verifying data returned from API is empty
            #     print(checkpoint)
            #     break
            
            # #appending data extracted to 
            # data_extracted.extend(checkpoint)
            # offset += limit

            # logging.info(f"Extracted data from API  from {specific_date} of kind {kind} and new offset is {offset}")
            
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {str(e)}")
        return None
    
    return data_extracted


def extract_book_data(book_id):
    """
    This function returns the data extracted in a list format from the API
    based on specific book id (OLID)

    book_id(str) : book olid extracted

    return a list of data extracted
    """
    data_extracted = []

    try:
        logging.info(f"Connecting to API to extract book data {book_id}")

        url = f"http://openlibrary.org/books/{book_id}.json"
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses

        data = response.json()

        data_extracted.append(data)  # Append extracted data
        logging.info(f"Extracted data  successfully from  API for book id {book_id}")
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {str(e)}")
    
    return data_extracted
    

#generate dates for data extraction using pandas
def generate_date(start_date, end_date):
    """
    This function generate dates based on 
    start date and end date 

    start_date(string) start_date in YYYY-MM-DD
    end_date(string) end_date in YYYY-MM-DD

    return a pandas datetimeindex
    """
    date_range = pd.date_range(start=start_date, end=end_date)
    return date_range


# Function to filter 'changes' column
def filter_books(changes_list):
    """
    Filter the 'changes' column to only include entries containing '/books/'

    changes_list(list): List of dictionaries containing changes

    return List of dictionaries containing only '/books/' entries
    """
    books_changes = [change for change in changes_list if isinstance(change, dict) and '/books/' in change.get('key', '')]
    return books_changes


# Function to extract '/books/book_id' from the 'changes' column
def extract_book(changes_list):
    """
    Extract '/books/book_id' from the 'changes' column

    changes_list (dict): List of dictionaries containing changes
    return: from '/books/book_id' return book_id (also know as OLID) or None
    """
    for change in changes_list:
        if '/books/' in change.get('key', ''):
            return change.get('key', '').split('/')[2]
    return None


def execute_sql_from_file(sql_file, connection):
    """
    Execute SQL commands from a file.

    sql_file (str): Path to the SQL file.
    connection (psycopg2.extensions.connection): PostgreSQL database connection.

    Returns: None
    """
    try:
        cursor = connection.cursor()

        # Open and read the SQL file
        with open(sql_file, 'r') as file:
            sql_script = file.read()

        # Execute the SQL script
        cursor.execute(sql_script)
        connection.commit()

        logging.info("SQL script executed successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error executing SQL script: %s", error)


def create_databse_connection():
 
    try:
        connection = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            dbname=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASSWORD"],
        )

        # Get the path to the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the SQL file
        sql_file_path = os.path.join(script_dir, 'create_tables.sql')

        # Call the function to execute the SQL script
        execute_sql_from_file(sql_file_path, connection)

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error connecting to PostgreSQL database: %s", error)

    finally:
        # Close the database connection
        if connection:
            connection.close()


if __name__ == "__main__":

    # generating dates based on requirements for project
    dates = generate_date(api_start_date,api_end_date)


    ####### DATA EXTRACTION 1 #######
    # extracting raw data from API
    final_df = data_extraction1(dates,extract_set,offset,limit)


    ####### DATA TRANSFORMATION #######
    # transforming data by removing null values, duplicate and also adding new fields
    final_df2 = data_transformation(final_df)


    final_df2.to_csv("bookdata.csv") # exporting book raw data for future use incase of backfill

    print(final_df2.info()) #checking columns and its data type


    ####### DATA EXTRACTION 2 #######
    # to extract data from the API based on the changes field for one book
    df_without_duplicates = data_extraction_2(final_df2)

    df_without_duplicates.to_csv("book_data_extracted.csv") # exporting book raw data for future use incase of backfill

    print(df_without_duplicates.info()) # checking columns and its data type

    #### CREATE TABLES ####
    # connecting to database to create the tables to matc
    create_databse_connection()




# if __name__ == "__main__":
#     print("Data Engineering Take Home Assessment")
#     connect_to_db()
