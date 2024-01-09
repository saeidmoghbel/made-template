import pandas as pd
import requests
import io
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt




url_2009 = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
url_2015 = "https://datamillnorth.org/download/road-traffic-accidents/df98a6dd-704e-46a9-9d6d-39d608987cdf/2015.csv"
url_2016 = "https://datamillnorth.org/download/road-traffic-accidents/b2c7ebba-312a-4b3d-a324-6a5eda85fa5b/Copy%2520of%2520Leeds_RTC_2016.csv"

def fetch_and_read(url):
    """
    Fetches data from the given URL and reads it as a CSV file.

    Args:
        url (str): The URL from which to fetch the data.

    Returns:
        pandas.DataFrame: The data read from the CSV file, or an empty DataFrame if there was an error.
    """
    try:
        response = requests.get(url)
        return pd.read_csv(io.StringIO(response.text), sep=";", error_bad_lines='skip')
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV from {url}: {e}")
        return pd.DataFrame()
    
def clean_datasets(cleaned_df):
    """
    Cleans and analyzes datasets.

    Args:
        cleaned_df (pandas.DataFrame): The cleaned dataset.

    Returns:
        pandas.DataFrame: The cleaned dataset.
    """
    df_1 = fetch_and_read(url_2009)
    df_2 = fetch_and_read(url_2015)
    df_3 = fetch_and_read(url_2016)
    combined_df = pd.concat([df_1, df_2, df_3], ignore_index=True)
    selected_columns = combined_df.loc[:, ["Casualty Severity", "Type of Vehicle"]]
    cleaned_df = clean_data(selected_columns)
    analyze_data(cleaned_df)
    return cleaned_df

def clean_data(df):
    """
    Cleans the given DataFrame by dropping rows with missing values in the 'Type of Vehicle' and 'Casualty Severity' columns,
    converting the values in these columns to lowercase, replacing specific values in the 'Type of Vehicle' column,
    and ordering the 'Casualty Severity' column.

    Args:
        df (pandas.DataFrame): The DataFrame to be cleaned.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """
    df.dropna(subset=["Type of Vehicle", "Casualty Severity"], inplace=True)
    df["Type of Vehicle"] = df["Type of Vehicle"].str.lower()
    df["Casualty Severity"] = df["Casualty Severity"].str.lower()
    df["Type of Vehicle"] = df["Type of Vehicle"].replace({"Taxi/Private hire car": "Car", "M/cycle 50cc and under": "Pedal cycle", "Other Vehicle": "Car",
                                                           "Motorcycle over 125cc and up to 500cc": "Pedal cycle", "Goods vehicle 3.5 tonnes mgw and under": "Bus or coach (17 or more passenger seats)"})
    severity_order = ["Slight", "Serious", "Fatal"]
    df["Casualty Severity"] = pd.Categorical(df["Casualty Severity"], categories=severity_order, ordered=True)
    return df

def analyze_data(cleaned_df):
    """
    Analyzes the given DataFrame by plotting a countplot of Casualty Severity for each Type of Vehicle.

    Parameters:
    cleaned_df (pandas.DataFrame): The DataFrame containing the cleaned data.

    Returns:
    None
    """
    selected_df = cleaned_df.loc[:, ["Casualty Severity", "Type of Vehicle"]]
    x=sns.countplot(x="Type of Vehicle", hue="Casualty Severity", data=selected_df)
    plt.title("Count of Casualty Severity for each Type of Vehicle")
    plt.plot(x)
    plt.show()

# Call the analyze_data function with the cleaned_df DataFrame
    analyze_data(cleaned_df)
    
def load_data(df, db_path, accidents, cleaned_df):
    """
    Load the given DataFrame into a SQLite database.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be loaded into the database.
    db_path (str): The path to the SQLite database file.
    accidents (str): The name of the table to be created in the database.
    cleaned_df (pandas.DataFrame): The cleaned DataFrame.

    Returns:
    None
    """
    conn = sqlite3.connect(db_path)
    df.to_sql(accidents, conn, index=False, if_exists='replace')
    conn.commit()
    conn.close()