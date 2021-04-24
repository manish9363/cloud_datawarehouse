import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load raw s3 data into staging tables.
    Copy command with proper roleand regions needs to be provided.
    
    """
    
    print("***********************************")
    print("INFO: Load Staging process started ")
    try:
        
        for query in copy_table_queries:
            print("Copy command for query :{}".format(query))
            
            print("load process has been started")
            
            cur.execute(query)
            conn.commit()
            
            print("INFO: load process has been completed")
            
    except Exception as e:
        print("Error message in load staging method is : {} ".format(e ))

def insert_tables(cur, conn):
    
    """
    Read the data from staging raw table and populate the analytics tables.
    
    """
    
    print("***********************************")
    print("INFO: Insert Table process started ")
    
    try:
        for query in insert_table_queries:
            print("INFO: Insert query is running : {} ".format(query))
            print("INFO: Insert process has been started")
            cur.execute(query)
            conn.commit()
            print("INFO: Insert process has been completed")
            
    except Exception as e:
        
        print("Error : Insert table function failed : Error Message : {} and query :{} ".format(e,query))
        
        
def main():
    """
    Reading the dwh config file.
    Establishing the connection with data base.
    creating the cursor 
    calling the load and insert function.
    
    """
    
    print("***********************************")
    
    print("******** Job Started **************")
    
    
    try:
        
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        
        
        print("Info: redshift database connection in progress")

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        print("Info: connected to redshift database")
    
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        conn.close()
        
        print("******************** Process is completed **************************")
        
    except Exception as e:
        
        print("Error: Error in main method is a hhhhh : {} ".format(e))


if __name__ == "__main__":
    main()