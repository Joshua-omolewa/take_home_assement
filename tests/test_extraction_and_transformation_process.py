from etl import *
import pandas as pd

# test cases
date_str = "2023/12/01"
date_conversion_to_timestamp= pd.to_datetime(date_str)
date_index = pd.DatetimeIndex([date_conversion_to_timestamp])
dates1 = date_index  
extract_set1 = ["add-cover"]  # sample book kind as per project requirement 

# request changes API restriction
offset1 = 0
limit = 1000

# =================================== API DATA EXTRACTION AND TRANSFORMATION TEST ================================================
# Test if the data from the API can be extracted and transformed without any issue 


def test__testing_extraction_transformation_process():

    """
    This function test the extraction and transformation process
    """

     # testing : extracting raw data from API
    book_df_raw = data_extraction1(dates1,extract_set1,offset1,limit)
    if book_df_raw.empty:
        assert not book_df_raw.empty, "No data was extracted from API please check API"

    # testing: transforming data by removing null values, duplicate and also adding new fields
    book_df = data_transformation(book_df_raw)

    # testing: to extract data from the API based on the changes field for each book id
    changes_book_df = data_extraction_2(book_df)

    assert changes_book_df is not None, f"data extraction from  API and data transformation failed"