import  psycopg2

def get_postgres_connection(db_config):
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']

    )
    return conn