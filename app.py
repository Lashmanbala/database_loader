import pandas as pd
import os
import dotenv
import sqlalchemy
import json

dotenv.load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

conn_uri = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sqlalchemy.create_engine(conn_uri)
connection = engine.connect()

#result1 = connection.execute(sqlalchemy.text('CREATE DATABASE retail_db'))


schema = json.load(open('data/retail_db/schemas.json'))

def get_column_names(schema,ds_name):
    clm_details = schema[ds_name]
    clm_names = sorted(clm_details, key= lambda col : col['column_position'])
    return [col['column_name'] for col in clm_names]

columns = get_column_names(schema, 'orders')

df_reader = pd.read_csv(
    'data/retail_db/orders/part-00000',
    names=columns,
    chunksize=10000
)

for idx, df in enumerate(df_reader):
    print(f'chunk size of chunk {idx} is {df.shape}')


'''try:
    df.to_sql(
        'orders',
        con=engine, 
        if_exists='replace', 
        index=False        # df has indecx in it. It shd be set to false. otherwise index'll be inserted as a column
    )
    print('succesfully created a table')
except Exception as e:
    print(e)

result2 = connection.execute(sqlalchemy.text('SHOW TABLES'))
databases_df = pd.DataFrame(result2.fetchall(), columns=result2.keys())
print(databases_df)
'''
