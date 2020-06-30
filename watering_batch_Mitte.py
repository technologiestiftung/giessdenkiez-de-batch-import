"""
Merge dataset of watered trees to tree ID's by attribute 'Pflegeobjekt' (watered trees)
on 'KENNZEICH', 'STANDORTNR' (db trees)
"""

import pandas as pd
from dotenv import load_dotenv
import os
import sys
import logging
import psycopg2

# set directory to dataset that contains watered trees
dataset_directory = 'data/bewaesserung_mitte_05_2020.csv'

# set metadata about the watering here
bezirk = 'Mitte'
name_of_administration = 'Bezirksamt Mitte'
amount = 200 # water in litres
watering_date = "2020-06-26"


# import watering datasets
df_wt = pd.read_csv(dataset_directory, sep = ';')
# split Pflegeobjekt string to seperate attributes
new = df_wt["Pflegeobjekt"].str.split(" ", n = 2, expand = True) 
df_wt['STANDORTNR'] = df_wt['Baum Nr'].astype(str)
df_wt['KENNZEICH'] = new[0]
df_wt['strname'] = new[2]
# clean dataset
df_wt = df_wt.drop(['Erledigt_am', 'Baum Nr', 'Pflegeobjekt'], axis=1)
df_wt['STANDORTNR'] = df_wt['STANDORTNR'].apply(lambda x: x.rstrip())

# import trees dataset (same as in the database)
trees = pd.read_csv('data/trees_extended.csv', sep=',')
# filter dataset to relevant district
trees = trees.loc[trees['bezirk'] == bezirk]
# clean dataset
trees['STANDORTNR'] = trees['STANDORTNR'].astype(str)
trees = trees.loc[:, ['id','KENNZEICH', 'STANDORTNR']]
trees['STANDORTNR'] = trees['STANDORTNR'].apply(lambda x: x.rstrip())

# merge watered trees and trees
watered_trees = pd.merge(trees, df_wt, on = ['KENNZEICH', 'STANDORTNR'], how = "inner")

# clean dataset and add columns with metadata
watered_trees['tree_id'] = watered_trees['id']
watered_trees = watered_trees.drop(['id','KENNZEICH','STANDORTNR','Baumart','strname'], axis = 1)
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


exit()

##### under construction

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
for env_var in ["PG_DB", "PG_PORT", "PG_USER", "PG_PASS", "PG_DB", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET"]:
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