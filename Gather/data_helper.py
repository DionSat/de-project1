import pandas as pd
import numpy as np
from datetime import datetime
import random
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from loguru import logger

def create_dataframe(df):
  df = df.mask(df == '')    # Replace blank values with NaN
  df = df.astype({"METERS":'int',
                  "GPS_LONGITUDE":'float',
                  "GPS_LATITUDE":'float'})    # Set type for important columns
  df.drop(['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'], axis=1, inplace = True)    # Drop unneeded columns for now. 
  df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:00:00:00')    # Change OPD_DATE values to datetime
  # Set the initial value of the new column to 0
  df['SPEED'] = 0

  # Calculate the speed for each row using the formula
  # (current meters - previous meters) / (current ACT_TIME - previous ACT_TIME)
  meters_diff = df[['METERS', 'VEHICLE_ID']].groupby("VEHICLE_ID").diff()
  time_diff = df[['ACT_TIME', 'VEHICLE_ID']].groupby("VEHICLE_ID").diff()
  meters_diff = meters_diff['METERS']
  time_diff = time_diff['ACT_TIME']
  df['SPEED'] = meters_diff / time_diff

  # Replace NaN and infinity values with 0
  df['SPEED'] = df['SPEED'].replace([np.nan, np.inf, -np.inf], 0)

  df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'])
  df['TIME_STAMP'] = (pd.to_datetime(df['OPD_DATE'], unit='s') + pd.to_timedelta(df['ACT_TIME'], unit='s'))
  return df

def create_stop_dataframe(df):
  df = df.mask(df == '')    # Replace blank values with NaN
  df = df.astype({"DIRECTION":'int',
                  "SERVICE_KEY":'char',
                  "ROUTE_ID":'int'})    # Set type for important columns
  # Find if uneeded columns exist. 
  return df

def data_assertions(df):
  # Assertion #1  If there is longitude, then there is latitude
  df1 = pd.DataFrame()
  df1[['GPS_LONGITUDE', 'GPS_LATITUDE']] = df[['GPS_LONGITUDE', 'GPS_LATITUDE']].copy(deep=True)
  df1 = df1[~df1['GPS_LONGITUDE'].isnull()]    # Get all rows with non empty longitude and put them in a dataframe
  try:
      assert not df1[df1['GPS_LATITUDE'].isnull()].values.any(), f"Some longitude doesn't have latitude"    #Check if there are any empty rows in latitude rows
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices =df1[df1['GPS_LATITUDE'].isnull()].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #2 Every record has a date and time
  df2 = pd.DataFrame()
  df2[['OPD_DATE', 'ACT_TIME']] = df[['OPD_DATE', 'ACT_TIME']].copy(deep=True)
  try:
      assert not df2[df2['OPD_DATE'].isnull() | df2['ACT_TIME'].isnull()].values.any(), f"Some records don't have a date or time"    #Find any rows that have empty OPD_DATE or ACT_TIME
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices =df2[df2['OPD_DATE'].isnull() | df2['ACT_TIME'].isnull()].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #3 Act_time should be greater than 0
  df3 = pd.DataFrame()
  df3[['ACT_TIME']] = df[['ACT_TIME']].copy(deep=True)
  try:
      assert not df3[(df3['ACT_TIME'].lt(0))].values.any(), f"Some records have actual time greater than 24 hours or less than 0"
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df3[df3['ACT_TIME'].lt(0)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #4 Dates should either be from the year 2022, or 2023
  df4 = pd.DataFrame()
  curr = datetime.now()
  df4[['OPD_DATE']] = df[['OPD_DATE']].copy(deep=True)
  try:
      assert not df4[(df4['OPD_DATE'] <= '2022-01-01') & (df4['OPD_DATE'] >= curr)].values.any(), f"Some dates are out of range"    #bool check every value in the column OPD_DATE that if they are before 2022 or after current datetime
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df4[(df4['OPD_DATE'] <= '2022-01-01') & (df4['OPD_DATE'] >= curr)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #5 Every record is on the same day
  df5 = pd.DataFrame()
  df5[['OPD_DATE']] = df[['OPD_DATE']].copy(deep=True)
  df5['OPD_DATE'] = df5['OPD_DATE'].dt.strftime('%d')    #Covert OPD_DATE to datatime and then extract the day field for the rows
  try:
      assert not df5[df5['OPD_DATE'] != df5['OPD_DATE'][0]].values.any(), f"Some dates are not on the same day"    #Check if all the values in the OPD_DATE field are the same day
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df5[(df5['OPD_DATE'] != df5['OPD_DATE'][0])].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #6 Longitude should be negative
  df6 = pd.DataFrame()
  df6[['GPS_LONGITUDE']] = df[['GPS_LONGITUDE']].copy(deep=True)
  df6 = df6[~df6['GPS_LONGITUDE'].isna()]    #Get all rows with non empty longitude and put them in a dataframe
  try:
      assert not df6[df6['GPS_LONGITUDE'].gt(0)].values.any(), f"Some record has a longitude that is positive"    #bool check every value in the column GPS_LONGITUDE that is positive and check if any of those values is true
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df6[df6['GPS_LONGITUDE'].gt(0)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #7 Latitude should be positive.
  df7 = pd.DataFrame()
  df7[['GPS_LATITUDE']] = df[['GPS_LATITUDE']].copy(deep=True)
  df7 = df7[~df7['GPS_LATITUDE'].isna()]    #Get all rows with non empty latitude and put them in a dataframe
  try:
      assert not df7[df7['GPS_LATITUDE'].lt(0)].values.any(), f"Some record has a latitude that is negative"    #bool check every value in the column GPS_LATITUDE that is negative and check if any of those values is true
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df7[df7['GPS_LATITUDE'].lt(0)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #8 Meters should be greater than or equal to 0
  df8 = pd.DataFrame()
  df8[['METERS']] = df[['METERS']].copy(deep=True)
  try:
      assert not df8[df8['METERS'].lt(0)].values.any(), f"Some record have traveled a negative distance relative to total distance"    #bool check every value in the column METERS that is less than 0 meters and check if any of those values is true
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df8[df8['METERS'].lt(0)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #9 The latitude and longitude exist on earth.
  df9 = pd.DataFrame()
  df9[['GPS_LONGITUDE', 'GPS_LATITUDE']] = df[['GPS_LONGITUDE', 'GPS_LATITUDE']].copy(deep=True)
  df9 = df9[~df9['GPS_LONGITUDE'].isna()]    # Get all rows with non empty longitude and put them in a dataframe
  df9 = df9[~df9['GPS_LATITUDE'].isna()]    # Get all rows with non empty latitude and put them in a dataframe
  try:
      assert not df9[df9['GPS_LONGITUDE'].lt(-180) | df9['GPS_LONGITUDE'].gt(180) | df9['GPS_LATITUDE'].gt(90) | df9['GPS_LATITUDE'].lt(-90)].values.any(), f"Some records longitude and latitude is not on earth"
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df9[df9['GPS_LONGITUDE'].lt(-180) | df9['GPS_LONGITUDE'].gt(180) | df9['GPS_LATITUDE'].gt(90) | df9['GPS_LATITUDE'].lt(-90)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")
  # Assertion #10 Speed should be greater than 0 but less than 90 m/s
  df10 = pd.DataFrame()
  df10[['SPEED']] = df[['SPEED']].copy(deep=True)
  try:
      assert not df10[df10['SPEED'].gt(45)].values.any(), f"Some records are going at unsafe high speeds"
  except AssertionError as error:
      error_message = f"AssertionError: {error}"
      error_indices = df10[df10['SPEED'].gt(45)].index.tolist()
      print(error_message)
      print(f"Error occurred in row(s): {error_indices[0]}")

def stop_data_assertions(df):

def data_splitter(df):
  breadcrumbs_df = df[['EVENT_NO_TRIP', 'TIME_STAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED']]
  breadcrumbs_df = breadcrumbs_df.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'TIME_STAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed'})

  trip_df = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].copy(deep=True)
  trip_df = trip_df.drop_duplicates(subset=["EVENT_NO_TRIP"])
  trip_df['route_id'] = np.round(trip_df['EVENT_NO_TRIP'] + random.randrange(10,30), 3)
  trip_df['service_key'] = np.round(trip_df['EVENT_NO_TRIP'] + random.randrange(10,30), 3)
  trip_df['direction'] = np.random.choice([0, 1])
  trip_df = trip_df.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'VEHICLE_ID': 'vehicle_id'})
  return breadcrumbs_df, trip_df

def data_write(data, file) -> None:
    """
    Store data in json files
    Return None
    """

    if not data:
        return None

    data = json.loads(data.decode("utf-8"))

    json.dump(data, file)
    file.write("\n")

def insert_db(breadcrumbs_df, trip_df):
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn_engine = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Create Temp table for trip
    trip_df.to_sql('TempTrip', con=conn_engine, if_exists='replace', index=False)

    # Insert TempTrip rows that are not present in Trip
    cursor.execute("""INSERT INTO "Trip" ("trip_id", "route_id", "vehicle_id", "service_key", "direction")
                      SELECT "trip_id", "route_id", "vehicle_id", "service_key", "direction"
                      FROM "TempTrip" t
                      WHERE NOT EXISTS 
                          (SELECT 1 FROM "Trip" f
                           WHERE t.trip_id = f.trip_id)""")

    # Append to the BreadCrumbs table
    breadcrumbs_df.to_sql('BreadCrumbs', con=conn_engine, if_exists='append',
          index=False)

    # Get the row count for BreadCrumbs table
    cursor.execute('SELECT count(*) from "BreadCrumbs";')
    result = cursor.fetchone()
    curr_bread = result[0]

    # Get the row count for Trip table
    cursor.execute('SELECT count(*) from "Trip";')
    result = cursor.fetchone()
    curr_trip = result[0]

    conn.close()
    return curr_bread, curr_trip

def insert_stop_db(breadcrumbs_df, trip_df):
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn_engine = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Append to the Stop table
    breadcrumbs_df.to_sql('Stop', con=conn_engine, if_exists='append',
          index=False)

    # Get the row count for BreadCrumbs table
    cursor.execute('SELECT count(*) from "Stop";')
    result = cursor.fetchone()
    curr_stop = result[0]

    conn.close()
    return curr_stop

def create_db(breadcrumbs_df, trip_df):
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    # Create new BreadCrumbs table
    breadcrumbs_df.to_sql('BreadCrumbs', con=conn, if_exists='replace',
          index=False)
    # Create new Trip table
    trip_df.to_sql('Trip', con=conn, if_exists='replace',
          index=False)

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Add Constraints
    cursor.execute('ALTER TABLE "Trip" ADD PRIMARY KEY ("trip_id");')
    cursor.execute('ALTER TABLE "BreadCrumbs" ADD CONSTRAINT "FK_trip" FOREIGN KEY("trip_id") REFERENCES "Trip"("trip_id");')

    # Get the row count for BreadCrumbs table
    cursor.execute('SELECT count(*) from "BreadCrumbs";')
    result = cursor.fetchone()
    curr_bread = result[0]

    # Get the row count for Trip table
    cursor.execute('SELECT count(*) from "Trip";')
    result = cursor.fetchone()
    curr_trip = result[0]

    conn.close()
    return curr_bread, curr_trip

def create_stop_db(stop_df):
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    # Create new Stop table
    stop_df.to_sql('Stop', con=conn, if_exists='replace',
          index=False)

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Add Constraints
    cursor.execute('ALTER TABLE "Stop" ADD CONSTRAINT "FK_trip" FOREIGN KEY("trip_id") REFERENCES "Trip"("trip_id");')

    # Get the row count for Stop table
    cursor.execute('SELECT count(*) from "stop";')
    result = cursor.fetchone()
    curr_stop = result[0]

    conn.close()
    return curr_stop

def delete_db():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop constraints and then drop the tables
    cursor.execute('ALTER TABLE "BreadCrumbs" DROP CONSTRAINT "FK_trip";')
    cursor.execute('ALTER TABLE "Trip" DROP CONSTRAINT "Trip_pkey";')
    cursor.execute('DROP TABLE "BreadCrumbs";')
    cursor.execute('DROP TABLE "Trip";')

    conn.close()
    print("DELETE Successful")

def stop_delete_db():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop constraints and then drop the tables
    cursor.execute('ALTER TABLE "Stop" DROP CONSTRAINT "FK_trip";')
    cursor.execute('DROP TABLE "Stop";')

    conn.close()
    print("DELETE Successful")

def db_rowcount():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    insp = inspect(db)

    # Check if BreadCrumbs table exists then get the row count
    if insp.has_table("BreadCrumbs", schema="public"):
        cursor.execute('SELECT count(*) from "BreadCrumbs";')
        result = cursor.fetchone()
        curr_bread = result[0]
    else:
        curr_bread = 0

    # Check if the Trip Table exists then get the row count
    if insp.has_table("Trip", schema="public"):
        cursor.execute('SELECT count(*) from "Trip";')
        result = cursor.fetchone()
        curr_trip = result[0]
    else:
        curr_trip = 0

    conn.close()
    return curr_bread, curr_trip

def db_stop_rowcount():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    insp = inspect(db)

    # Check if Stop table exists then get the row count
    if insp.has_table("Stop", schema="public"):
        cursor.execute('SELECT count(*) from "Stop";')
        result = cursor.fetchone()
        curr_stop = result[0]
    else:
        curr_stop = 0

    conn.close()
    return curr_stop

def check_tables():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    insp = inspect(db)

    # Check if BreadCrumbs table exists then get the row count
    flag = insp.has_table("BreadCrumbs", schema="public") and insp.has_table("Trip", schema="public")

    conn.close()
    return flag

def check_stop_table():
    conn_string = "postgresql+psycopg2://postgres:breadcrumbs@localhost:5432/postgres"

    db = create_engine(conn_string)
    conn = db.connect()

    conn = psycopg2.connect(host="localhost", user="postgres", password = "breadcrumbs",database = "postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    insp = inspect(db)

    # Check if Stop table exists then get the row count
    flag = insp.has_table("Stop", schema="public")

    conn.close()
    return flag
