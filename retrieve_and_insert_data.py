import os
from dotenv import load_dotenv
import requests
import pandas as pd
from io import BytesIO
from pymongo import MongoClient

load_dotenv()

# URL to download the dataset
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-04.parquet"

# Download the dataset
response = requests.get(url)
response.raise_for_status()

# Read the dataset into a DataFrame
df = pd.read_parquet(BytesIO(response.content))

# MongoDB connection string for MongoDB Atlas
mongodb_uri = os.getenv("MONGODB_URI")

# Insert the DataFrame into MongoDB
client = MongoClient(mongodb_uri)
db = client["BigDataFinalProject"]
collection = db["NYC_Taxi"]
collection.insert_many(df.to_dict(orient="records"))

# Close the MongoDB connection
client.close()

print("Data inserted into MongoDB Atlas successfully.")
