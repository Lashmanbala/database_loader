import pandas as pd
import os
import dotenv
import sqlalchemy
import json
import glob
import sys

def get_column_names(schema,ds_name):
    clm_details = schema[ds_name]
    clm_names = sorted(clm_details, key= lambda col : col['column_position'])
    return [col['column_name'] for col in clm_names]

def read_csv(file, schema):
    file_path_list = file.split('/')
    ds_name = file_path_list[-2]
    columns = get_column_names(schema, ds_name)
    df_reader = pd.read_csv(file,names=columns,chunksize=10000)
    return df_reader

def to_sql(df, db_conn_engine, ds_name):
    df.to_sql(ds_name, 
              db_conn_engine, 
              if_exists='append', 
              index=False)

def db_loader(ds_name, schema, db_conn_engine, SRC_BASE_DIR):
    files = glob.glob(f'{SRC_BASE_DIR}/{ds_name}/part-*')

    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for file in files:
        df_reader = read_csv(file, schema)

        for idx, df in enumerate(df_reader):
            print(f'processing chunk {idx} of {ds_name}')
            to_sql(df, db_conn_engine, ds_name)

def process_files(ds_names=None):
    dotenv.load_dotenv()

    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    SRC_BASE_DIR = os.getenv('SRC_BASE_DIR')

    conn_uri = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    db_conn_engine = sqlalchemy.create_engine(conn_uri)

    schema = json.load(open(f'{SRC_BASE_DIR}/schemas.json'))

    if not ds_names:
        ds_names = schema.keys()

    for ds_name in ds_names:
        try:
            print(f'processing {ds_name}')
            db_loader(ds_name, schema, db_conn_engine, SRC_BASE_DIR)
        except NameError as ne:
            print(ne)
        except Exception as e:
            print(e)
        finally:
            print(f'complted processing {ds_name}')

if __name__ == '__main__':
    if len(sys.argv) == 2:
        print(sys.argv[1])
        ds_names = json.loads(sys.argv[1]) # argv returns a list with the filename and arguments as its elements.
                                            # run time args should be in json array format. 
                                            # Ex:'["categories", "products"]'  Ex: "[\"categories\",\"product\",\"departments\"]"
        process_files(ds_names)
    else:
        process_files()


'''conn_uri = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
db_conn_engine = sqlalchemy.create_engine(conn_uri)
connection = db_conn_engine.connect()

result2 = connection.execute(sqlalchemy.text('SHOW TABLES'))
databases_df = pd.DataFrame(result2.fetchall(), columns=result2.keys())
print(databases_df)'''
