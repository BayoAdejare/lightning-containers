import os
import json
import pandas as pd
from shapely.geometry import Polygon, Point, MultiPolygon

us_state_cb = "img/carto_boundaries/gz_2010_us_040_00_20m.json"  # carto boundaries file

# Load boundary shapefile
data = json.load(open(us_state_cb))
df = pd.DataFrame(data["features"])
# Extract the required field from the GeoJSON
df["Location"] = df["properties"].apply(lambda x: x["NAME"])
df["Type"] = df["geometry"].apply(lambda x: x["type"])
df["Coordinates"] = df["geometry"].apply(lambda x: x["coordinates"])

# Create Polygon or MultiPolygon objects depending on the States type.
df_new = pd.DataFrame()

for idx, row in df.iterrows():

    if row["Type"] == "MultiPolygon":
        list_of_polys = []
        df_row = row["Coordinates"]
        for ll in df_row:
            list_of_polys.append(Polygon(ll[0]))
        poly = MultiPolygon(list_of_polys)

    elif row["Type"] == "Polygon":
        df_row = row["Coordinates"]
        poly = Polygon(df_row[0])

    else:
        poly = None

    row["Polygon"] = poly
    df_new = pd.concat([df_new, pd.DataFrame([row])], ignore_index=True)

# Drop columns we don't need
df_selection = df_new.drop(columns=["type", "properties", "geometry", "Coordinates"])

# Feed in an example lat and long and see the results - if its not right first time, switch your lat and long ;-)
point = Point(-155.03677, 29.1661)  # Example GPS location for somewhere in Florida
state = df_selection.apply(
    lambda row: row["Location"] if row["Polygon"].contains(point) else None, axis=1
).dropna()

print(f"State is: {state}")
