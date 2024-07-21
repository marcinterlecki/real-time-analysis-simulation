import os
import pandas as pd
import numpy as np
import urllib
from sqlalchemy import create_engine
from fastparquet import ParquetFile

# Step 1: Load Parquet data into a pandas DataFrame
folder_path = 'df' # specify your folder path here
parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

# Step 2: Establish a connection to your Azure SQL database
params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=xxxxx;'
    r'DATABASE=xxxxx;'
    r'UID=xxxxx;'
    r'PWD=xxxxx'
)


engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))

for file in parquet_files:
    df = pd.read_parquet(os.path.join(folder_path, file))
    # Convert lists in 'Decision' to strings
    df['Decision'] = df['Decision'].apply(lambda x: ', '.join(map(str, x)))

    # Step 3: Insert data into the database using pandas' to_sql method
    for chunk in np.array_split(df, 100): # break the data into smaller chunks for efficient upload
        chunk.to_sql('Decision', engine, if_exists='append', index=False) # replace <table_name> with your table name
