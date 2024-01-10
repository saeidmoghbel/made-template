import pytest
import sqlite3
import pandas as pd
from pipeline import fetch_and_read, clean_data, load_data

def test_valid_url():
    url_2009 = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
    url_2015 = "https://datamillnorth.org/download/road-traffic-accidents/df98a6dd-704e-46a9-9d6d-39d608987cdf/2015.csv"
    url_2016 = "https://datamillnorth.org/download/road-traffic-accidents/b2c7ebba-312a-4b3d-a324-6a5eda85fa5b/Copy%2520of%2520Leeds_RTC_2016.csv"
    df_1 = fetch_and_read(url_2009)
    df_2 = fetch_and_read(url_2015)
    df_3 = fetch_and_read(url_2016)
    df = pd.concat([df_1, df_2, df_3], ignore_index=True)
    assert not df.empty

def test_invalid_url():
    url = "invalid url"
    df = fetch_and_read(url)
    assert df.empty

def test_nan_dropped():
    df = pd.DataFrame({"Type of Vehicle": ["car_1", "car_2", "bus", "cycle", None],
                       "Casualty Severity": ["slight", "serious", "fatal", None]})
    cleaned_df = clean_data(df)
    assert pd.notna(cleaned_df).all().all()

def test_columns_lowercase():
    df = pd.DataFrame({"Type of Vehicle": ["car_1", "car_2", "bus", "cycle"],
                       "Casualty Severity": ["slight", "serious", "fatal"]})
    cleaned_df = clean_data(df)
    assert all(value == value.lower() for value in cleaned_df["Type of Vehicle"].unique())

def test_data_loaded():
    df = pd.DataFrame({"Type of Vehicle": ["car_1", "car_2", "bus", "cycle"],
                       "Casualty Severity": ["slight", "serious", "fatal"]})
    cleaned_df = clean_data(df)
    conn = sqlite3.connect('./data/accidents.sqlite')
    query = "SELECT * FROM accidents LIMIT 5;"
    table_data = pd.read_sql(query, conn)
    load_data(cleaned_df, conn, 'accidents')
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    assert not table_data.empty, "accidents data is empty"
    conn.close()
    
def main():
    test_valid_url()
    test_invalid_url()
    test_nan_dropped()
    test_columns_lowercase()
    test_data_loaded()
if __name__ == "__main__":
    pytest.main()