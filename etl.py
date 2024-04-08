

def data_extraction1(dates,kind,offset, limit):

    """
    This function extract the data from API,
    loads data into a data frame and returns the data frame

    dates(list) dates range
    kind(str): book kind in in the set (add-cover, add-book, edit-book, merge-authors)
    offset_value(int): specifying API offset
    limit_value(int): specifying API limit

    returns dataframe
    """
    # to avoid Circular imports I need to import other functions from app.py
    from app import extract_data
    import pandas as pd
    import logging

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
    """

    # to avoid Circular imports I need to import other functions from app.py
    from app import filter_books
    from app import extract_book
    import pandas as pd
    import logging

    final_df2 = final_df.copy() # creating a copy of final dataframe

    logging.info("Tranforming data to meet project requirment")


    final_df2['changes'] = final_df2['changes'].apply(filter_books) # Filtering daframe changes column to only include books

    final_df_filtered_copy = final_df2.copy() # creating a copy of filtered final dataframe

    final_df_filtered_copy.loc[:, 'book'] = final_df2['changes'].apply(extract_book) #adding a new column called books to contained extracted book id

    # Drop rows with missing values only in specific columns
    final_df_filtered_copy = final_df_filtered_copy.dropna(subset=['book'])

    logging.info("data succesfully transfromed")

    return final_df_filtered_copy


def data_extraction_2(final_df2):

    """
    This function extract the book data from API,
    loads data into a data frame, do some transformation 
    to remove duplicates and missing values
    and returns the transformed data frame

    final_df2(dataframe)

    returns dataframe
    """

    # to avoid Circular imports I need to import other functions from app.py
    from app import extract_book_data
    import pandas as pd
    import logging

   # Create an empty list to store extracted data
    extracted_data2 = []

    # Iterate over 'book' column in final_df_filtered_copy
    for book_id in  final_df2['book']:
        # Extract data for each book ID
        book_data = extract_book_data(book_id)
        if book_data is not None:
            # Extend extracted_data list with data for this book along with the book ID
            for data in book_data:
                data['book'] = book_id
            extracted_data2.extend(book_data)


    second_final_df  = pd.DataFrame(extracted_data2)
    # # print(second_final_df)
   

    # Drop duplicates based on book column
    df_without_duplicates = second_final_df.drop_duplicates(subset=['book'])

    # Drop rows with missing values in book column
    df_without_missing_values = df_without_duplicates.dropna(subset=['book'])

    return df_without_missing_values