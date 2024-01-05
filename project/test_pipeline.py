import pytest
import sqlite3
import pandas as pd
from pipeline import fetch_and_read, clean_data, load_data

def test_valid_url():
    url = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
    df = fetch_and_read(url)
    assert not df.empty

def test_invalid_url():
    url = "invalid url"
    df = fetch_and_read(url)
    assert df.empty

def test_nan_dropped():
    df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus or coach (17 or more passenger seats)", "Pedal cycle", None],
                       "Casualty Severity": ["Slight", "Serious", "Fatal", None]})
    cleaned_df = clean_data(df)
    assert pd.notna(cleaned_df).all().all()

def test_columns_lowercase():
    df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus or coach (17 or more passenger seats)", "Pedal cycle"],
                       "Casualty Severity": ["Slight", "Serious", "Fatal"]})
    cleaned_df = clean_data(df)
    assert all(value == value.lower() for value in cleaned_df["Type of Vehicle"].unique())

def test_data_loaded():
    df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus or coach (17 or more passenger seats)", "Pedal cycle"],
                       "Casualty Severity": ["Slight", "Serious", "Fatal"]})
    cleaned_df = clean_data(df)
    conn = sqlite3.connect(':memory:')
    load_data(cleaned_df, conn, 'combined_accidents')
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    assert ('combined_accidents',) in tables
    conn.close()