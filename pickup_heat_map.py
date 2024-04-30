import os
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
import geopandas as gpd
import folium
from folium.plugins import HeatMap
from shapely.geometry import Polygon

load_dotenv()

# MongoDB Atlas connection URI
mongodb_uri = os.getenv("MONGODB_URI")

# Connect to MongoDB Atlas
client = MongoClient(mongodb_uri)
db = client["BigDataFinalProject"]
collection = db["NYC_Taxi"]

# Load TLC Taxi Zone GeoJSON file
taxi_zones_geojson = "NYC_Taxi_Zones.geojson"
taxi_zones = gpd.read_file(taxi_zones_geojson)

# Aggregate pickup and dropoff counts by TLC Taxi Zone ID
pipeline = [
    {"$group": {"_id": "$PULocationID", "pickup_count": {"$sum": 1}}},
    {"$project": {"_id": 0, "location_id": {"$toString": "$_id"}, "pickup_count": 1}}
]
pickup_counts_cursor = collection.aggregate(pipeline)
pickup_counts = pd.DataFrame(pickup_counts_cursor)

# Fill NaN values with 0
pickup_counts.fillna(0, inplace=True)

# Merge pickup counts with taxi zones
taxi_zones = taxi_zones.merge(pickup_counts, left_on='location_id', right_on='location_id', how='left')

# Fill NaN values with 0
taxi_zones.fillna(0, inplace=True)

m = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

# Create a HeatMap for pickups
pickup_heatmap_data = [[row['geometry'].centroid.y, row['geometry'].centroid.x, row['pickup_count']] for idx, row in taxi_zones.iterrows() if row['location_id'] in pickup_counts['location_id'].values]
HeatMap(pickup_heatmap_data, radius=10, blur=20).add_to(m)

# Save the map as an HTML file
m.save("pickup_heatmap.html")
