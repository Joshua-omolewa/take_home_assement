import os
from app import database_connection_test


# =================================== DATABASE CONNECTION TEST ================================================
# Test if the connection to the database is successful 

def test__database_connection_test__testing_if_connection_works():
    """
    testing to see if the connection to our destination (database) is working
    """

    assert database_connection_test() , f"conection to database unsuccessful"