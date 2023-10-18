import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data = data.copy()
    new_data['dob'] = new_data["Date of Birth"]
    new_data['Birth-Month, Birth-Day, Birth-Year'] = new_data.dob.str.split(' ', expand=True)
    new_data.drop(columns = ["dob", "Date of Birth"], inplace = True)
    return new_data

def load_data(data):
    db_url = "postgresql+psycopg2://max:lot230510@db:5432/shelter
    conn = create_engine(db_url)
    data.to_sql("outcomes", conn, if_exists="append", index=False)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Doing the thing")
    df = extract_data(args.source)
    temp_df = transform_data(df)
    load_data(temp_df, args.target)
    print("Done doing the thing")






























