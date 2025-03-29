import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import os

# Variables
FOLDER_NAME = 'data'
DATA_FOLDER =  Path(__file__).parent.parent.joinpath(FOLDER_NAME)
DB_NAME = 'f1_analytics.db'
DB_PATH = Path(__file__).parent.parent.joinpath(FOLDER_NAME).joinpath(DB_NAME)

engine = create_engine(f'sqlite:///{DB_PATH}')

def load_csv_to_db():
    for file in os.listdir(DATA_FOLDER):
        if file.endswith('.csv'):
            table_name = file.replace('.csv', '')
            file_path = os.path.join(DATA_FOLDER, file)
            df = pd.read_csv(file_path)
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f'Values have been loaded to table {table_name}.')

if __name__ == '__main__':
    load_csv_to_db()
