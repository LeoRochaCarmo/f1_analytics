#%%

import pandas as pd
import kagglehub
import sqlalchemy
import os
from pathlib import Path
import shutil

# Download latest version
path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")

print("Path to dataset files:", path)

folder_name = 'data'
dir_path =  Path(__file__).parent.joinpath(folder_name)

# creating folder path for the files
os.makedirs(dir_path, exist_ok=True)

# moving all files to new folder
for file in os.listdir(path):
    origin = os.path.join(path, file)
    file_destination = os.path.join(dir_path, file)

    shutil.move(origin, file_destination)

print(f'files moved to {Path.absolute(dir_path)}')

folder_name ='data'
db_name = 'f1_analytics.db'
db_path = Path(__file__).parent.joinpath(folder_name).joinpath(db_name)

engine = sqlalchemy.create_engine(f'sqlite:///{db_path}')

for file in os.listdir(dir_path):
    if file.endswith('.csv'):
        file_path = os.path.join(dir_path, file)
        table_name = file.replace('.csv', '')

        df = pd.read_csv(file_path)

        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f'Table {table_name} created.')

print('Database created!')

#%%

import pandas as pd
import kagglehub
import sqlalchemy
import os
from pathlib import Path
import shutil

# Download latest version
path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")
print("Path to dataset files:", path)

# Variables
folder_name = 'data'
dir_path =  Path(__file__).parent.joinpath(folder_name)
folder_name ='data'
db_name = 'f1_analytics.db'
db_path = Path(__file__).parent.joinpath(folder_name).joinpath(db_name)


def moving_files(path,dir_path):
    # creating folder path for the files
    os.makedirs(dir_path, exist_ok=True)
    # moving all files to new folder
    for file in os.listdir(path):
        origin = os.path.join(path, file)
        file_destination = os.path.join(dir_path, file)

        shutil.move(origin, file_destination)

    return f'files moved to {Path.absolute(dir_path)}'


def db_creation(dir_path, db_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{db_path}')

    for file in os.listdir(dir_path):
        if file.endswith('.csv'):
            file_path = os.path.join(dir_path, file)
            table_name = file.replace('.csv', '')

            df = pd.read_csv(file_path)

            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f'Table {table_name} created.')
    
    return 'Database created!'


moving_files(path, dir_path)
db_creation(dir_path, db_path)

# %%

