import kaggle_download
import db_schema
import load_data
from time import sleep
def main():
    print('Pipeline of F1 data begins...')
    kaggle_download.moving_files()
    db_schema.create_database()
    load_data.load_csv_to_db()
    print('Pipeline has ended.')

main()