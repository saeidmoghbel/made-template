import sqlalchemy
import pandas as pd
import sqlite3

from sqlalchemy import create_engine, Column, Float, BIGINT, Text, MetaData
url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"
dataset = pd.read_csv(url, sep=";", on_bad_lines='skip')

column_type = {
    "column_1": BIGINT(),
    "column_2": Text(),
    "column_3": Text(),
    "column_4": Text(),
    "column_5": Text(),
    "column_6": Text(),
    "column_7": Float(),
    "column_8": Float(),
    "column_9": BIGINT(),
    "column_10": Float(),
    "column_11": Text(),
    "column_12": Text(),
    "geo_punkt": Text()
}

engine = create_engine("sqlite:///airports.sqlite")
dataset.to_sql('airports', engine, index=False, if_exists='replace',dtype = column_type)