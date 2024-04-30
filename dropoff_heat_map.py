import pandas as pd
from pymongo import MongoClient
import geopandas as gpd
import folium
from folium.plugins import HeatMap
from shapely.geometry import Polygon

# MongoDB Atlas connection URI
mongodb_uri = "mongodb+srv://rahman_big_data:B7O53FIMRV5tdKF7@mongodb.12spgep.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB"

# Connect to MongoDB Atlas
client = MongoClient(mongodb_uri)
db = client["BigDataFinalProject"]
collection = db["NYC_Taxi"]

# Load TLC Taxi Zone GeoJSON file
taxi_zones_geojson = "NYC_Taxi_Zones.geojson"
taxi_zones = gpd.read_file(taxi_zones_geojson)

# Aggregate dropoff counts by TLC Taxi Zone ID
pipeline = [
    {"$group": {"_id": "$DOLocationID", "dropoff_count": {"$sum": 1}}},
    {"$project": {"_id": 0, "location_id": {"$toString": "$_id"}, "dropoff_count": 1}}
]
dropoff_counts_cursor = collection.aggregate(pipeline)
dropoff_counts = pd.DataFrame(dropoff_counts_cursor)

# Fill NaN values with 0
dropoff_counts.fillna(0, inplace=True)

# Merge dropoff counts with taxi zones
taxi_zones = taxi_zones.merge(dropoff_counts, left_on='location_id', right_on='location_id', how='left')

# Fill NaN values with 0
taxi_zones.fillna(0, inplace=True)

m = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

# Create a HeatMap for dropoffs
dropoff_heatmap_data = [[row['geometry'].centroid.y, row['geometry'].centroid.x, row['dropoff_count']] for idx, row in taxi_zones.iterrows() if row['location_id'] in dropoff_counts['location_id'].values]
HeatMap(dropoff_heatmap_data, radius=45, blur=30).add_to(m)

# Save the map as an HTML file
m.save("dropoff_heatmap.html")