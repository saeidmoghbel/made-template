import requests
import pytest
import sqlite3
import pandas as pd
from unittest.mock import patch, Mock
import numpy as np
from pipeline import fetch_and_read, clean_data, analyze_data, load_data

class TestDataFetching:
    @patch('pipeline.requests.get')
    def test_valid_url(self, mock_get):
        mock_get.return_value.text = 'Type of Vehicle,Casualty Severity\nCar,Slight'
        url = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
        df = fetch_and_read(url)
        assert not df.empty

    @patch('pipeline.requests.get')
    def test_invalid_url(self, mock_get):
        mock_get.side_effect = requests.RequestException
        url = "invalid url"
        df = fetch_and_read(url)
        assert df.empty

class TestDataProcessing:
    def test_nan_dropped(self):
        df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus", "Taxi", np.nan],
                           "Casualty Severity": ["Slight", "Serious", "Fatal", np.nan]})
        cleaned_df = clean_data(df)
        assert pd.notna(cleaned_df).all().all()

    def test_columns_lowercase(self):
        df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus", "Taxi"],
                           "Casualty Severity": ["Slight", "Serious", "Fatal"]})
        cleaned_df = clean_data(df)
        assert all(value == value.lower() for value in cleaned_df["Type of Vehicle"].unique())

class TestDataLoading:
    def test_data_loaded(self):
        df = pd.DataFrame({"Type of Vehicle": ["Car", "Bus", "Taxi"],
                           "Casualty Severity": ["Slight", "Serious", "Fatal"]})
        cleaned_df = clean_data(df)
        conn = sqlite3.connect(':memory:')
        load_data(cleaned_df, conn, 'combined_accidents')
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        assert ('combined_accidents',) in tables
        conn.close()