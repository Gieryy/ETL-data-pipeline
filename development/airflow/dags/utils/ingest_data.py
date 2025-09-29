import psycopg2
from airflow.models import Variable
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pg_connection():
    
    pg_host = Variable.get("PG_HOST")
    pg_port = Variable.get("PG_PORT")
    pg_user = Variable.get("PG_USERNAME")
    pg_password = Variable.get("PG_PASSWORD")
    pg_dbname = Variable.get("PG_DBNAME")

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname,
    )
    
    return conn

def insert_data(table_name, data, conn):
    
    cursor = conn.cursor()
    
    ### Create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        customer_id VARCHAR(255),
        customer_name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    
    ### Insert data
    for row_data in data:
        
        insert_query = f"""
        INSERT INTO {table_name} (customer_id, customer_name)
        VALUES (%s, %s);
        """
        
        cursor.execute(insert_query, (row_data['customer_id'], row_data['customer_name'],))
        conn.commit()
        
        print(f"Inserted data into {table_name}: {row_data['customer_id']} - {row_data['customer_name']}")
        
    cursor.close()
    
def insert_data_df(table_name, data):
    
    df = pd.DataFrame(data)
    
    df['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # engine = create_engine("postgresql://your_user:your_password@localhost:5432/your_db")
    engine = create_engine(f"postgresql://{Variable.get('PG_USERNAME')}:{Variable.get('PG_PASSWORD')}@{Variable.get('PG_HOST')}:{Variable.get('PG_PORT')}/{Variable.get('PG_DBNAME')}")
    
    logger.info(f"Data to be inserted into {table_name}:")
    logger.info(df.head())

    try:
        df.to_sql("new_customers_df", engine, if_exists="append", index=False, chunksize=500)
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}")
        raise
    
    
def get_data_customers(conn):
    
    query = "SELECT * FROM public.customers"
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    # print(cursor.description)
    column_name = [desc[0] for desc in cursor.description]
    
    row_data = cursor.fetchall()
    
    data = [dict(zip(column_name, row)) for row in row_data]
    
    return data

def get_data(table, new_table_name):
    
    if table == "customers":
        conn = pg_connection()
        data = get_data_customers(conn)
        
        print(f"Data fetched from customers table: {data}")
        
        
        # insert_data(new_table_name, data, conn)
        insert_data_df(new_table_name, data)
        
        conn.close()