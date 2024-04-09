#Author: Joshua Omolewa

import pandas as pd
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

### THE SECTION BELOW CONTAINS FUNCTIONS USED FOR THE ETL PROCESS ##

def data_extraction1(dates,kind,offset, limit):

    """
    This function extract the data from API,
    loads data into a data frame and returns the data frame

    dates(DatetimeIndex) dates range generated using pandas range_date method
    kind(str): book kind in in the set (add-cover, add-book, edit-book, merge-authors)
    offset_value(int): specifying API offset
    limit_value(int): specifying API limit

    returns dataframe
    """
    # to avoid Circular imports I need to import other functions from app.py
    from app import extract_data


    extracted_data_frames = []  # To store DataFrames for each endpoint's data
    extract_set = kind

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

    return final_df


def data_transformation(final_df):

    """
    This function take the dataframe a returns the transfromed dataframe
    data cleaning was performed to removed duplicates and missing values

    final_df (pandas.DataFrame)

    return: none
    """

    # to avoid Circular imports I need to import other functions from app.py
    from app import filter_books
    from app import extract_book

    final_df2 = final_df.copy() # creating a copy of final dataframe

    logging.info("Tranforming data to meet project requirment")


    final_df2['changes'] = final_df2['changes'].apply(filter_books) # Filtering daframe changes column to only include books

    final_df_filtered_copy = final_df2.copy() # creating a copy of filtered final dataframe

    final_df_filtered_copy.loc[:, 'book'] = final_df2['changes'].apply(extract_book) #adding a new column called books to contained extracted book id

    # Drop rows with missing values only in in book column
    final_df_filtered_copy = final_df_filtered_copy.dropna(subset=['book'])

    # Drop duplicates based on book column
    df_without_duplicates = final_df_filtered_copy.drop_duplicates(subset=['book'])

    logging.info("data succesfully transfromed")

    return df_without_duplicates


def data_extraction_2(final_df2, batch_size=100, max_workers=4):
    """
    This function extracts the book data from API,
    loads data into a DataFrame, performs transformations 
    to remove duplicates and missing values, and returns the transformed DataFrame.

    final_df2 (pandas.DataFrame): Input DataFrame containing book IDs.
    batch_size (int): Number of book IDs to request in each batch.
    max_workers (int): Number of threads to use for concurrent execution.

    return:Transformed DataFrame.
    """
    # to avoid Circular imports I need to import other functions from app.py
    from app import extract_book_data
  
    # Function to process a batch of book IDs
    def process_batch(book_ids):
        batch_data = []
        for book_id in book_ids:
            book_data = extract_book_data(book_id)
            if book_data:
                for data in book_data:
                    data['book'] = book_id
                batch_data.extend(book_data)
        return batch_data

    # Split book IDs into batches which returns a list of list of book ids in batches. So i query the API in batches
    book_batches = [final_df2['book'][i:i+batch_size] for i in range(0, len(final_df2['book']), batch_size)]

    # Generator function to yield results as they become available
    def generate_results():
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_batch, batch) for batch in book_batches]
            for future in futures:
                yield from future.result() # because I am using an asynchronous generator

    # Create DataFrame from yielded results
    second_final_df = pd.DataFrame(generate_results())

    return second_final_df