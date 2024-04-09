from app import load_data,create_databse_tables
from etl import *
import pandas as pd
from clean_test_tables_for_test import clean_database_test_tables

# test cases
date_str = "2023/12/01"
date_conversion_to_timestamp= pd.to_datetime(date_str)
date_index = pd.DatetimeIndex([date_conversion_to_timestamp])
dates1 = date_index  
extract_set1 = ["add-cover"]  # sample book kind as per project requirement 

# request changes API restriction
offset1 = 0
limit = 1000

# =================================== DATA LOADING TEST ================================================
# Test if the data from the API can be loaded into the database after the extraction and transformation process without an error

def test__loading_data_into_database():

    """
    Testing loading of data from API into test_tables
    """
     # testing : extracting raw data from API
    book_df_raw = data_extraction1(dates1,extract_set1,offset1,limit)
    if book_df_raw.empty:
        assert not book_df_raw.empty, "No data was extracted from API please check API"

    # testing: transforming data by removing null values, duplicate and also adding new fields
    book_df = data_transformation(book_df_raw)

    # testing: to extract data from the API based on the changes field for each book id
    changes_book_df = data_extraction_2(book_df)
    if changes_book_df.empty:
        assert not changes_book_df.empty, "No data was extracted from API for book id please check API"

    # testing connecting to database to create the tables based on DDL script (test_create_tables.sql)
    create_test_tables = create_databse_tables('test_create_tables.sql')
    if not create_test_tables:
        assert create_test_tables, f"Unable to create test tables in database"

    
    # testing: load data into the books table
    load_data_into_test_book_table = load_data(changes_book_df,"test_books")
    if not load_data_into_test_book_table:
        assert load_data_into_test_book_table, f"unable to load data into test_books"

    # testing: load data into the request_changes table   
    load_data_into_test_request_changes = load_data(book_df,"test_request_changes")
    if not load_data_into_test_request_changes:
        assert load_data_into_test_request_changes, f"unable to load data into test_request_changes"

    clean_database_test_tables()