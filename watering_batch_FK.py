"""
Merge dataset of watered trees to tree ID's by geometries (watered trees --> 'Hochwert', 'Rechtswert'
on db trees --> 'lat', 'lng')
"""

import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point
from dotenv import load_dotenv
import os
import sys
import logging
import psycopg2

# set directory to dataset that contains watered trees
dataset_directory = 'data/bewaesserung_sbk_fk_28_05_2020.csv'

# set metadata about the watering here
bezirk = 'Friedrichshain-Kreuzberg'
name_of_administration = 'Bezirk Friedrichshain-Kreuzberg'
amount = 200 # water in litres
watering_date = "2020-06-26"

# import watering datasets
df_wt = pd.read_csv(dataset_directory, sep = ';')
# clean dataset
df_wt = df_wt.loc[:, ['Hochwert','Rechtswert','Pflanzjahr']]
df_wt['Hochwert'] = df_wt['Hochwert'].astype(float)
df_wt['Rechtswert'] = df_wt['Rechtswert'].astype(float)
# create geodataframe from watering dataset
df_wt = GeoDataFrame(
    df_wt.drop(['Rechtswert', 'Hochwert'], axis=1),
    crs={'init': 'epsg:25833'},
    geometry=[Point(xy) for xy in zip(df_wt.Rechtswert, df_wt.Hochwert)])

# import trees dataset (same as in the database)
trees = pd.read_csv('data/trees_extended.csv', sep=',')
# filter dataset to relevant district
trees = trees.loc[trees['bezirk'] == bezirk]
# clean dataset
trees = trees.loc[:, ['id','lat','lng','pflanzjahr']]
# create geodataframe from db trees dataset
trees = GeoDataFrame(
    trees.drop(['lat', 'lng'], axis=1),
    crs={'init': 'epsg:4326'},
    geometry=[Point(xy) for xy in zip(trees.lat, trees.lng)])
trees = trees.to_crs("EPSG:25833")
# create buffer around db trees
trees['geometry'] = trees.geometry.buffer(1.2)

# match watered trees that are within the buffer of tb trees
watered_trees = GeoDataFrame({"id":[],'geometry':[]})
within_trees = GeoDataFrame()
for index, row in trees.iterrows():
    test = GeoDataFrame(row)
    within_trees = df_wt[df_wt.geometry.within(row['geometry'])]
    if within_trees.empty:
        print(index)
        continue
    within_trees = row
    within_trees = within_trees.drop(labels=['pflanzjahr'])
    watered_trees = watered_trees.append(within_trees)
    print("matched")
    within_trees = GeoDataFrame()

# export shapefile for validation   
watered_trees.to_file(driver = 'ESRI Shapefile', filename= "kreuzberg1.shp")

# clean dataset and add columns with metadata
watered_trees['tree_id'] = watered_trees['id']
watered_trees = watered_trees.drop(['id','geometry'], axis = 1)
watered_trees['time'] = watering_date + " 12:00:00.000000+00"
watered_trees['uuid'] = 'b_w_'+ bezirk[:4]
watered_trees['amount'] = amount
watered_trees['timestamp'] = pd.Timestamp.now()
watered_trees['username'] = name_of_administration

# print matching result
print('For', len(watered_trees), 'of', len(df_wt), 'watered trees an ID could be found.')

# import old watered trees batch dataset
twb = pd.read_csv('trees_watered_batch.csv', sep=',')
twb.to_csv('backup/trees_watered_batch_bu.csv', sep=",", index=False)

# concat old and new
watered_trees = pd.concat([twb,watered_trees])
watered_trees = watered_trees.drop_duplicates('tree_id')

# export watered trees
watered_trees.to_csv('trees_watered_batch.csv', sep=",", index=False)

values= watered_trees.values.tolist()

# import data in db
# setting up logging
logging.basicConfig()
LOGGING_MODE = None
if "LOGGING" in os.environ:
  LOGGING_MODE = os.getenv("LOGGING")
  if LOGGING_MODE == "ERROR":
    logging.root.setLevel(logging.ERROR)
  elif LOGGING_MODE == "WARNING":
    logging.root.setLevel(logging.WARNING)
  elif LOGGING_MODE == "INFO":
    logging.root.setLevel(logging.INFO)
  else:
    logging.root.setLevel(logging.NOTSET)
else:
  logging.root.setLevel(logging.NOTSET)

# loading the environmental variables
load_dotenv()

# check if all required environmental variables are accessible
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB"]:
  if env_var not in os.environ:
    logging.error("Environmental Variable {} does not exist".format(env_var))

# database connection
pg_server = os.getenv("PG_SERVER")
pg_port = os.getenv("PG_PORT")
pg_username = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASS")
pg_database = os.getenv("PG_DB")
dsn = f"host='{pg_server}' port={pg_port} user='{pg_username}' password='{pg_password}' dbname='{pg_database}'"
try:
  conn = psycopg2.connect(dsn)
  logging.info("Database connection established")
except:
  logging.error("Could not establish database connection")
  conn = None

with conn.cursor() as cur:
  cur.execute("DELETE FROM public.trees_watered_batch;")
  psycopg2.extras.execute_batch(
    cur,
    "INSERT INTO public.trees_watered_batch (tree_id, time, uuid, amount, timestamp, username) VALUES (%s, %s, %s, %s, %s, %s);",
    values
  )
  conn.commit()
        
conn.close()