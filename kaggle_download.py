import kagglehub
import os
from pathlib import Path
import shutil

# Variables
FOLDER_NAME = 'data'
DIR_PATH =  Path(__file__).parent.joinpath(FOLDER_NAME)
FOLDER_NAME ='data'
DB_NAME = 'f1_analytics.db'
DB_PATH = Path(__file__).parent.joinpath(FOLDER_NAME).joinpath(DB_NAME)
PATH = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")

def moving_files():
    # creating folder path for the files
    os.makedirs(DIR_PATH, exist_ok=True)
    # moving all files to new folder
    for file in os.listdir(PATH):
        origin = os.path.join(PATH, file)
        file_destination = os.path.join(DIR_PATH, file)

        shutil.move(origin, file_destination)

    return f'files moved to {Path.absolute(DIR_PATH)}'

if __name__ == '__main__':
    moving_files()

