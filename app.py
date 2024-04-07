#Author: Joshua Omolewa

#importing relevant libraries an modules for project
import os
import logging 
import requests
import time
import psycopg2
from dotenv import load_dotenv
import pandas as pd 

# configuring logging
logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

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
        logging.info(f"Extract data from  API for book id {book_id}")
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

if __name__ == "__main__":

    #generating date based on requirement for project
    dates = generate_date(api_start_date,api_end_date)

    ## DATA EXTRACTION ##
    # data = {}  
    # for single_date in dates:
    #     formatted_date = single_date.strftime("%Y/%m/%d")
    #     for single_kind  in extract_set:
    #         data_for_endpoint = {}
    #         extracted_data = extract_data(formatted_date, single_kind,offset, limit)
    #         if extracted_data is not None:
    #                             data_for_endpoint[formatted_date] = extracted_data
    #     data[single_kind] = data_for_endpoint

    ## DATA EXTRACTION ##
    ####### DATA EXTRACTION 1 #######
    extracted_data_frames = []  # To store DataFrames for each endpoint's data

    for single_date in dates:
        formatted_date = single_date.strftime("%Y/%m/%d")
        for single_kind in extract_set:
            extracted_data = extract_data(formatted_date, single_kind, offset, limit)
            if extracted_data is not None:
                # Convert extracted data into a DataFrame
                df = pd.DataFrame(extracted_data)
                # Add columns for date and kind
                df['date'] = formatted_date
                df['kind'] = single_kind
                # Append the DataFrame to the list
                extracted_data_frames.append(df)

    # Concatenate all DataFrames into a single DataFrame
    final_df = pd.concat(extracted_data_frames, ignore_index=True)

    #changing datatype to timestamp for date columns
    final_df['date'] = pd.to_datetime(df['date'])
    final_df['timestamp'] = pd.to_datetime(df['timestamp'])

    ####### DATA TRANSFORMATION #######

    final_df2 = final_df.copy() # creating a copy of final dataframe


    final_df2['changes'] = final_df2['changes'].apply(filter_books) # Filtering daframe changes column to only include books

    final_df_filtered_copy = final_df2.copy() # creating a copy of filtered final dataframe

    final_df_filtered_copy.loc[:, 'book'] = final_df2['changes'].apply(extract_book) #adding a new column called books to contained extracted book id

    # Drop rows with missing values only in specific columns
    final_df_filtered_copy = final_df_filtered_copy.dropna(subset=['book'])

    
    final_df_filtered_copy .to_csv("table1.csv")

    print(final_df_filtered_copy.columns)
    print(final_df_filtered_copy.info())


    ####### DATA EXTRACTION 2 #######

    # Create an empty list to store extracted data
    extracted_data2 = []

    # Iterate over 'book' column in final_df_filtered_copy
    for book_id in final_df_filtered_copy['book']:
        # Extract data for each book ID
        book_data = extract_book_data(book_id)
        if book_data is not None:
            # Extend extracted_data list with data for this book along with the book ID
            for data in book_data:
                data['book'] = book_id
            extracted_data2.extend(book_data)


    second_final_df  = pd.DataFrame(extracted_data2)
    # print(second_final_df)
    second_final_df.to_csv("table2_with_duplicate.csv")

    # Drop duplicates based on one column
    df_without_duplicates = second_final_df.drop_duplicates(subset=['book'])

    # Drop rows with missing values only in specific columns
    df_without_missing_values = df_without_duplicates.dropna(subset=['book'])

    # print(df_without_duplicates)
    df_without_duplicates.to_csv("table2_without_duplicate.csv")

    print(df_without_missing_values.columns)
    print(df_without_missing_values.info())




# if __name__ == "__main__":
#     print("Data Engineering Take Home Assessment")
#     connect_to_db()
