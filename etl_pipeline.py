
import pandas as pd
import sqlite3
import os

# Author: Huzaifa Khan

def extract_data(file_path):
    print("Extracting data...")
    df = pd.read_csv(file_path)
    return df

def transform_data(df):
    print("Transforming data...")
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    df.dropna(inplace=True)
    return df

def load_data(df, database_name):
    print("Loading data into database...")
    conn = sqlite3.connect(database_name)
    df.to_sql('data_table', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data successfully loaded into {database_name}")

def run_etl():
    input_file = 'data/sample_data.csv'
    output_db = 'output/data.db'
    os.makedirs('output', exist_ok=True)
    df = extract_data(input_file)
    df = transform_data(df)
    load_data(df, output_db)

if __name__ == "__main__":
    run_etl()
