#Author Joshua Omolewa
from app import extract_data

# test case
date = '2023/12/01'  # date chosen and must be in YYYY-MM-DD
kind = "add-cover"  # kind as per project requirement could be 

# request changes API restriction
offset = 0
limit = 1000


# =================================== API CONNECTION TEST ================================================
# Test if the connection to the API is successful as it should return a list of response

def test__extract_data__for_data_extraction_from_api():

    """"
    This test if the request API end point works as API could also return empty list 
    """
    response = extract_data(date, kind, offset, limit)

    if response == None:
        assert response,f"The connection to the request changes API is not successful"
    elif response == []:
        assert response == [], f"The connection to the request changes API is not successful"
    else: 
        assert response, f"The connection to the request changes API is not successful"