import os
import psycopg2
import logging


def clean_database_test_tables():
    """
    This function is used to clean up the tables created from the loading test scrippt
    """
    try:
        connection = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            dbname=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASSWORD"],
        )
        logging.info("connection to database successful")

        cur = connection.cursor() #creating database cursor

        # Drop foreign key constraint in the child table
        cur.execute("ALTER TABLE test_request_changes DROP CONSTRAINT IF EXISTS fk_test_request_changes_book;")

        # Drop the child table
        cur.execute("DROP TABLE IF EXISTS test_request_changes;")

        # Drop the parent table
        cur.execute("DROP TABLE IF EXISTS test_books;")

        # Commit the transaction
        connection.commit()

        logging.info("Tables dropped successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        # Log any errors that occur during the process
        logging.error(f"Error dropping tables: {error}")
        connection.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        connection.close()
