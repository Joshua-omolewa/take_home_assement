#Author: Joshua Omolewa

#importing relevant modules, packages and libraries for project
import os
import logging 
from io import StringIO
import requests
import pytest
import psycopg2
import dotenv
from dotenv import load_dotenv
import pandas as pd 
from etl import * # importing all funciton from our ETL script

# configuring logging
logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s',
                        datefmt = "%m/%d/%Y  %I:%M:%S %p")

# Load the environment variables 
load_dotenv()


# project requirement 
extract_set = ["add-cover", "add-book", "edit-book", "merge-authors"]
api_start_date = "2023-12-01"
api_end_date = "2023-12-03"

# API restriction 
offset = 0
limit = 1000


### THE SECTION BELOW CONTAINS SOME FUNCTIONS USED FOR THE PROJECT PROCESS ###
# OTHER FUNCTIONS CAN BE FOUND IN THE elt.py SCRIPT

def extract_data(specific_date, kind, offset_value, limit_value):
    """
    This function returns the data extracted in a list format from the API
    based on specific date and using API restrictions including offset to
    paginate through response

    specific_date(str) : date in YYYY/MM/DD
    kind(str): book kind in in the set (add-cover, add-book, edit-book, merge-authors)
    offset_value(int): specifying API offset
    limit_value(int): specifying API limit

    return: a list of data extracted
    """
    data_extracted = []
    offset = offset_value
    limit = limit_value
    try:
        while True:
            logging.info(f"Connecting to API to extract data from {specific_date} of kind {kind} and current offset is {offset}")

            params = {"limit": limit , "offset":offset} # query string for API
            # params = {"limit": limit } # query string for API for testing purpose only and not required

            url = f"http://openlibrary.org/recentchanges/{specific_date}/{kind}.json"
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise HTTPError for bad responses

            data = response.json()

            # data_extracted.extend(data) # for testing purpose only and not required
            # break #for testing purpose only and not required

            # checking if data returned is empty as API returns a list so I can stop data extraction based on offset
            checkpoint = data if isinstance(data, list) else [data]

            
            if not checkpoint:
                #Verifying data returned from API is empty
                print(checkpoint)
                break
            
            #appending data extracted to data_extracted list
            data_extracted.extend(checkpoint)
            offset += limit

            logging.info(f"Extracted data from API  from {specific_date} of kind {kind} and new offset is {offset}")
            
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {str(e)}")
        return None
    
    return data_extracted

def database_connection_test():

    try:
        connection = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            dbname=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASSWORD"],
        )
        logging.info("connection to database successful")
        return True

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"connection to database not successful {error}")
        return None




def extract_book_data(book_id):
    """
    This function returns the data extracted in a list format from the API
    based on specific book id (OLID)

    book_id(str):  book olid extracted

    return: a list of data extracted
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
        return None
    
    return data_extracted
    

#generate dates for data extraction using pandas
def generate_date(start_date, end_date):
    """
    This function generate dates based on 
    start date and end date 

    start_date(string) start_date in YYYY-MM-DD
    end_date(string) end_date in YYYY-MM-DD

    return: a pandas datetimeindex
    """
    date_range = pd.date_range(start=start_date, end=end_date)
    return date_range


# Function to filter 'changes' column
def filter_books(changes_list):
    """
    Filter the 'changes' column to only include entries containing '/books/'

    changes_list(list): List of dictionaries containing changes

    return: List of dictionaries containing only '/books/' entries
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

    return: None
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
        logging.error(f"Error executing SQL script: {error}")
        return None


def create_databse_tables(sql_script):
    
    """
    Create database tables by executing SQL commands from a file.

    This function establishes a connection to a PostgreSQL database using environment variables
    for host, database name, user, and password. It then retrieves the path to an SQL file
    ('create_tables.sql') located in the same directory as the script and executes the SQL commands
    from that file using the `execute_sql_from_file` function.
    sql_script(str): name of sql DDL script

    return: True if successful, None otherwise
    """

    try:
        # establish connection to the database
        connection = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            dbname=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASSWORD"],
        )

        # Get the path to the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the SQL file
        sql_file_path = os.path.join(script_dir, sql_script)

        # Call the function to execute the SQL script
        execute_sql_from_file(sql_file_path, connection)
        return True

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error connecting to PostgreSQL database: {error}")
        return None

    finally:
        # Close the database connection
        if connection:
            connection.close()

def load_data(df, table_name):
    """
    Load data from a Pandas DataFrame into a PostgreSQL table using COPY command.

    This function takes a DataFrame  and a table name as input. It converts
    the DataFrame into CSV format using a string buffer and then inserts the data into the specified
    PostgreSQL table using the COPY command.


    df (pandas.DataFrame): The DataFrame containing the data to be inserted.
    table_name (str): The name of the PostgreSQL table where the data will be inserted.

    return: True if successful, None otherwise
    """

    # Create a string buffer
    buffer = StringIO()

    # Write the DataFrame to the buffer as CSV data with header
    df.to_csv(buffer, index=False, na_rep='NULL')

    buffer.seek(0)  # Reset buffer position

    try:
        # establish connection to the database
        connection = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            dbname=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASSWORD"],
        )

        # Create a cursor object
        cur = connection.cursor()

        # Get table columns
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        table_columns = [row[0] for row in cur.fetchall()]

        # Fill missing columns in DataFrame with NULL
        for col in table_columns:
            if col not in df.columns:
                df[col] = 'NULL'

        # showing the missing columns for tracking schema change while loading
        missing_columns = set(table_columns) - set(df.columns)
        if missing_columns:
            logging.warning(f"The following columns are missing from the DataFrame: {', '.join(missing_columns)}")

        # Write the DataFrame to the buffer again after filling missing columns with NULL
        buffer.seek(0)
        buffer.truncate(0)
        df.to_csv(buffer, index=False, na_rep='NULL')

        buffer.seek(0)  # Reset buffer position

        # Using the COPY command to load data as copy it is faster than insert into DML statment
        # I use the copy_expert method of the cursor (cur) to execute the COPY command with more control over the options.
        # COPY table_name FROM STDIN: This tells PostgreSQL to copy data from the standard input (our string buffer) into the specified table (table_name).
        # WITH CSV HEADER: This indicates that the CSV data includes a header row, which PostgreSQL should use to determine the column names.
        # NULL 'NULL': This specifies that empty strings in the CSV file should be interpreted as NULL values in the database. 
        # This addresses the issue where empty values are represented by commas in the CSV file.
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER NULL 'NULL'", buffer)

        # Commit the transaction
        connection.commit()
        logging.info(f"Data inserted into table {table_name} successfully.")
        return True
    except Exception as error:
        # Log any errors that occur during the process
        logging.error(f"Error inserting data into table {table_name}: {error}")
        connection.rollback()
        return None
    finally:
        # Close the cursor and connection
        cur.close()
        connection.close()

if __name__ == "__main__":

    ############################ Runing  Unit tests ##########################

    retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"]) # this variable store the exit code from pytest  

    # Fail the cell execution if there are any test failures.
    # As per pytest documentation: Exit code 0: All tests were collected and passed successfully
    assert retcode == 0, "The pytest test failed. See the log for details."

    logging.info('All unit tests passed. Initiating the ELT process')

    # generating dates based on requirements for project
    dates = generate_date(api_start_date,api_end_date)

   

    ###################### PERFORMING THE ETL PROCESS ################################

    ####### DATA EXTRACTION - STEP 1 #######
    # extracting raw data from API
    book_df_raw = data_extraction1(dates,extract_set,offset,limit)


    ####### DATA TRANSFORMATION - STEP 2 #######
    # transforming data by removing null values, duplicate and also adding new fields
    book_df = data_transformation(book_df_raw)


    book_df.to_csv("bookdata.csv", index=False) # exporting book raw data for future use incase of backfill
    # print(book_df.info()) #checking columns and its data type


    ####### DATA EXTRACTION - STEP 3 #######
    # to extract data from the API based on the changes field for each book id
    changes_book_df = data_extraction_2(book_df)

    changes_book_df.to_csv("book_data_extracted.csv",index=False) # exporting book raw data for future use incase of backfill
    # print(changes_book_df.info()) # checking columns and its data type


    #### CREATE TABLES - STEP 4 ####
    # connecting to database to create the tables based on DDL script (create_tables.sql)
    create_databse_tables('create_tables.sql')


    #### LOADING DATA INTO DATABASE - STEP 5 ####
    # connecting to database and loading data into the database tables

    # load data into the books table
    load_data(changes_book_df,"books")

    # load data into the request_changes table
    load_data(book_df,"request_changes")
