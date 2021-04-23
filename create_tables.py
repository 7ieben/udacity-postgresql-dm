import psycopg2
from sql_queries import create_table_queries, create_type_queries, drop_table_queries, drop_type_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def execute_query_list(cur, conn, query_list):
    """
    Execute the queries in query_list using curser cur on connection conn.
    """
    try:
        for query in query_list:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print("Error executing query list")
        print(e)

        
def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
      cursor to it.
    
    - Drop all the types.    
    - Create all the types.    
    - Drops all the tables.
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    try:
        cur, conn = create_database()
    
        execute_query_list(cur, conn, drop_type_queries)
        execute_query_list(cur, conn, create_type_queries)
    
        execute_query_list(cur, conn, drop_table_queries)
        execute_query_list(cur, conn, create_table_queries)
    finally:
        conn.close()


if __name__ == "__main__":
    main()