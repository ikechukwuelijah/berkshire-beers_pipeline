#%% Step 1: Fetch Data from API
# Import the requests library for handling HTTP requests
import requests

url = "https://beer9.p.rapidapi.com/"

querystring = {"brewery":"Berkshire brewing company"}

headers = {
	"x-rapidapi-key": "531193f132msh71720d21c9c014ap118541jsn0984870f4075",
	"x-rapidapi-host": "beer9.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

# %%
import pandas as pd
try:
    # Parse the JSON response
    json_data = response.json()

    # Check if the response contains the 'data' key and it is a list
    if 'data' in json_data and isinstance(json_data['data'], list):
        # Create a DataFrame from the 'data' key
        df = pd.DataFrame(json_data['data'])

        # Display the first few rows of the DataFrame
        print("Transformed DataFrame:\n", df.head())

        # Optional: Save to a CSV file
        df.to_csv("berkshire_beers.csv", index=False)
        print("Data saved to 'berkshire_beers.csv'")
    else:
        print("Unexpected response structure. 'data' key missing or not a list.")
        print("Response content:", json_data)

except ValueError as e:
    print("Error decoding JSON:", e)
    print("Response text:", response.text)
except requests.RequestException as e:
    print("Request error:", e)
    
#%% Step 3: Load Data into a Database

# Import necessary libraries for database operations
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Define the database connection string for connecting to SQL Server
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ARMSTRONG;"
    "Database=beerDB;"
    "Trusted_Connection=yes;"
)

# Set up logging to track script activity
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add console logging for real-time feedback
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        # Create an SQLAlchemy engine for database connection
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        # Upload the DataFrame to the database table
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        # Log any errors that occur during the upload process
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")


# Specify the table name and upload type
table_name = "berkshire-beers_table"
upload_type = "append"  # Options: 'replace', 'append'

# Upload the transformed data to the database
try:
    upload_data(table_name, df, upload_type)
    logging.info("Data uploaded successfully.")
    print("Data uploaded successfully.")
except Exception as e:
    logging.error(f"Failed to upload data: {e}")
    print(f"Failed to upload data: {e}")

# Log the end of the script
logging.info("Script ended.")

# %%
