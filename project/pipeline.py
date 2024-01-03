import pandas as pd
import sqlalchemy
import requests
import io
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt



url_2009 = "https://datamillnorth.org/download/road-traffic-accidents/288d2de3-0227-4ff0-b537-2546b712cf00/2009.csv"
url_2015 = "https://datamillnorth.org/download/road-traffic-accidents/df98a6dd-704e-46a9-9d6d-39d608987cdf/2015.csv"
url_2016 = "https://datamillnorth.org/download/road-traffic-accidents/b2c7ebba-312a-4b3d-a324-6a5eda85fa5b/Copy%2520of%2520Leeds_RTC_2016.csv"

def fetch_and_read (url):
    try:
        response = requests.get(url)
        return pd.read_csv(io.StringIO(response.text), sep =";" , on_bad_lines='skip')
    except requests.RequestException as e:
        print(f"the fetching data from {url}: {e}")
        pd.DataFrame()
    except pd.errors.ParserError as e:
        print(f"error parsing csv from {url}: {e}")
        pd.DataFrame()
            
    
df_1 = fetch_and_read(url_2009)
df_2 = fetch_and_read(url_2015)
df_3 = fetch_and_read(url_2016)

"""""
df_2009 = df_1.loc[ :, ['Type of Vehicle', 'Casualty Severity']]
df_2015 = df_2.loc[ :, ['Type of Vehicle', 'Casualty Severity']]
df_2016 = df_3.loc[ :, ['Type of Vehicle', 'Casualty Severity']]

df_2009 = df_2009.astype('category')
df_2015 = df_2015.astype('category')
df_2016 = df_2016.astype('category')

df_2009.dropna(subset=['Type of Vehicle'], inplace = True)
df_2015.dropna(subset=['Type of Vehicle'], inplace = True)
df_2016.dropna(subset=['Type of Vehicle'], inplace = True)

df_2009.dropna(subset=['Casualty Severity'], inplace = True)
df_2015.dropna(subset=['Casualty Severity'], inplace = True)
df_2016.dropna(subset=['Casualty Severity'], inplace = True)

df_2009 = df_2009.str.lower()
df_2015 = df_2015.str.lower()
df_2016 = df_2016.str.lower()
"""""

combined_df = pd.concat([df_1, df_2, df_3], ignore_index = True)
combined_df.dropna(subset=["Type of Vehicle", "Casualty Severity"], inplace = True)

combined_df["Type of Vehicle"] = combined_df["Type of Vehicle"].str.lower()
combined_df["Casualty Severity"] = combined_df["Casualty Severity"].str.lower()

combined_df = combined_df.head()

correlation_data = pd.crosstab(combined_df["Type of Vehicle"], combined_df["Casualty Severity"])
sns.heatmap(correlation_data, annot=True, cmap= 'coolwarm', fmt = 'd')
plt.title("Correlation between Type of vehicle and Casualty severity")
plt.show()


conn = sqlite3.connect('./data/accidents.sqlite')

combined_df.to_sql('combined_accidents', conn, index=False, if_exists='replace')

conn.commit()
conn.close()