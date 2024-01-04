import pytest
import sqlite3
import pandas as pd
from pipeline import fetch_and_read, transform_and_store, process_data
import unittest
from unittest.mock import patch
import numpy as np

class TestDataFetching(unittest.TestCase):
    def test_valid_url(self):
        url = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
        combined_df = fetch_and_read(url)
        assert combined_df.shape[0] > 0

    def test_invalid_url(self):
        url = "invalid url"
        combined_df = fetch_and_read(url)
        assert combined_df.empty

class TestDataProcessing(unittest.TestCase):
    def test_nan_dropped(self):
        combined_df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus", "Taxi", np.nan],
                                     "Casualty Severity": ["Slight", "Serious", "Fatal"]})
        processed_df = process_data(combined_df)
        assert pd.notna(processed_df).all().all()

    def test_columns_lowercase(self):
        combined_df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus", "Taxi"],
                                     "Casualty Severity": ["Slight", "Serious", "Fatal"]})
        processed_df = process_data(combined_df)
        assert all(col == col.lower() for col in processed_df.columns)

def test_fetch_and_read(sample_data):
    df_2009, df_2015, df_2016 = sample_data

    assert not df_2009.empty, "2009 Dataset is empty, fetching failed"
    assert not df_2015.empty, "2015 Dataset is empty, fetching failed"
    assert not df_2016.empty, "2016 Dataset is empty, fetching failed"

def test_transform_and_store(sample_data):
    df_2009, df_2015, df_2016 = sample_data

    conn = sqlite3.connect(':memory:')

    try:
        transform_and_store(df_2009, 'accidents_2009', conn)
        transform_and_store(df_2015, 'accidents_2015', conn)
        transform_and_store(df_2016, 'accidents_2016', conn)

        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        assert 'combined_accidents' in tables['name'].values
        

    finally:
        conn.close()