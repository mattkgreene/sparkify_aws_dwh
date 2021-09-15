import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schemas_queries, drop_schemas_queries
from etl import run_stg_load


def create_schemas(cur, conn):
    '''
    Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
    
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in create_schemas_queries:
        print ('executing query = {}'.format(query))
        cur.execute(query)
        conn.commit()        

def drop_schemas(cur, conn):
    '''
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_schemas_queries:
        print ('executing query = {}'.format(query))
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    '''
    Function to drop tables. This function uses the variable 'drop_table_queries' defined in the 'sql_queries.py' file.
    If run stage load is false then don't drop events_log_stg or song_log_stg as we don't want to process the data again.
    
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_table_queries:
        if (run_stg_load == False and query == drop_table_queries[0]):
            continue
        elif(run_stg_load == False and query == drop_table_queries[1]):
            continue
        else:
            print ('executing query = {}'.format(query))
            cur.execute(query)
            conn.commit()


def create_tables(cur, conn):
    '''
    Function to create schemas. This function uses the variable 'create_table_queries' defined in the 'sql_queries.py' file.
    if run stage load is false then don't create events_log_stg or song_log_stg as we don't want to process the data again.
    
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in create_table_queries:
        if(run_stg_load == False and query == create_table_queries[0]):
            continue
        elif(run_stg_load == False and query == create_table_queries[1]):
            continue
        else:
            print ('executing query = {}'.format(query))
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print ('Starting Drop Schemas')
    drop_schemas(cur, conn)
    print ('Starting Create Schemas')
    create_schemas(cur, conn)
    
    print ('Starting Drop Tables')
    drop_tables(cur, conn)
    print ('Starting Create Tables')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()