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

print(f"DB_USER: {DB_USER}")
print(f"DB_PASSWORD: {DB_PASSWORD}")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_PORT: {DB_PORT}")
print(type(DB_PORT))


conn_uri = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}'
#conn_uri1 = mysql+pymysql://remote_user:1234@192.168.129.94:3306
engine = sqlalchemy.create_engine(conn_uri)

#databases = pd.read_sql(sqlalchemy.text('SHOW DATABASES'), engine)
# Use the engine's connect method to execute a query
with engine.connect() as connection:
    result = connection.execute(sqlalchemy.text("SHOW DATABASES"))
    databases = result.fetchall()
    # Convert result to DataFrame
    databases_df = pd.DataFrame(databases, columns=result.keys())

# Print the result
print(databases_df)

schema = json.load(open('data/retail_db/schemas.json'))

def get_column_names(schema,ds_name):
    clm_details = schema[ds_name]
    clm_names = sorted(clm_details, key= lambda col : col['column_position'])
    return [col['column_name'] for col in clm_names]

print(get_column_names(schema,'departments'))