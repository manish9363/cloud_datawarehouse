import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    Droping the tables from the database
    
    """
    
    
    print("******************* Drop Table Process has been started *****************")
    try:
        for query in drop_table_queries:
            print("INFO: Query : {} drop table has been started")
            cur.execute(query)
            conn.commit()
            print("INFO: Query : {} drop table has been completed")
        print("******************* Drop Table Process has been Completed *****************")        
    except Exception as e:
        print("Error: drop table function , Error Message: {}".format(e))

def create_tables(cur, conn):
    
    """
    Creating the table structure in database.
    
    """
    
    print("******************* Create Table Process has been started *****************")
    try:
        
        for query in create_table_queries:
            
            print("INFO: Query : {} Create table has been started".format(query))
            cur.execute(query)
            conn.commit()
            print("INFO: Query : {} Create table has been completed".format(query))
            
    except Exception as e:
        
        print("Error: Create table function , Error Messgage :{}".format(e))


def main():
    
    """
    Creating connection and calling drop and create table structure.
    
    """
    
    try:
        
    
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        
    except Exception as e:
        print("Error: main program failed due to error : {}".format(e))
        


if __name__ == "__main__":
    main()