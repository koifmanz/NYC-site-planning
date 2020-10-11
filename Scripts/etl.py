import configparser
import numpy
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine
import sys


# Helper function

pd.set_option('mode.chained_assignment', None)

def read_file(fpath, ftype, col="pk_id", pk=True):
    """
    Description: read a csv/json file to df and add id field
    Arguments:
        fpath: a file path
        ftype: file type - json/csv
        col: a name for the new id
        pk: create new pk
    Returns:
        df: a panda data frame
    """
        
    if ftype == "json":
        df = pd.read_json(fpath)
        if pk:
            df[col] = range(1, 1+len(df))
        return df
    elif ftype == "csv":
        df = pd.read_csv(fpath)
        if pk:
            df[col] = range(1, 1+len(df))
        return df

def get_param(config_file):
    """
    Description: extract postgres connection parmaters from config file
    Arguments:
        config_file: path to file
    Returns:
        a dict of paramters
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    
    host = config.get('postgres', 'host')
    database = config.get('postgres', 'database')
    username = config.get('postgres', 'user')
    password = config.get('postgres', 'password')
    port = config.get('postgres', 'port')
    
    param_dic = {
    "host"      : host,
    "database"  : database,
    "user"      : username,
    "password"  : password,
    "port"      : port
    }
    
    return param_dic 

def get_files_path(config_file):
    """
    Description: extract csv/json files path from config file
    Arguments:
        config_file: path to file
    Returns:
        a dict of paths
    """
    config = configparser.ConfigParser()
    config.read("../finalData/config.cfg")
    trips_path = config.get('data', 'trips')
    trips_path
    
    files_loc = {
        "trips"            : config.get('data', 'trips'),
        "nyc_grid"         : config.get('data', 'nyc_grid'),
        "census_tracts"    : config.get('data', 'census_tracts'),
        "census_block_loc" : config.get('data', 'census_block_loc'),
        "taxi_zones"       : config.get('data', 'taxi_zones')
    }
    
    return files_loc


def connect(params_dic):
    """
    Description: Connect to the PostgreSQL database server
    Arguments:
        params_dic: a list of the parameters to connect to postgres
    Returns:
        conn: a connection to postgres db
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn


def execute_values(conn, df, table, truncate=True):
    """
    Description: Using psycopg2.extras.execute_values() to insert the dataframe into postgres
    Arguments:
        conn: a connection to postgres db
        df: pandas df to load 
        table: postgres table to insert the data into 
    Returns:
        None
    """
    
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        if truncate:
            print(f"TRUNCATE TABLE {table}")
            trucate_query  = f"TRUNCATE TABLE {table};"
            cursor.execute(trucate_query)
        print(f"Inserting values into {table}")
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()



def merge_census(census, blocks, county="County", census_key="CensusTract", new_pk="census_id"):
    """
    Description: an helper function to merge census data with coordinates
    Arguments:
        census: a pandas census dataframe
        blocks: a pandas census point location dataframe
        county: a field in blocks to filter the nyc boros boros
        census_key: census primary key 
        
    Returns:
        pandas data frame with location data
    """
    boros = ['Bronx','Kings','New York','Queens','Richmond']
    blocks_df = blocks[blocks[county].isin(boros)]
    blocks_df['tract'] = blocks_df.BlockCode // 10000
    census_with_xy = blocks_df.merge(census, left_on='tract', right_on=census_key)
    census_with_xy[new_pk] = range(1, 1+len(census_with_xy))

    return census_with_xy


def check_empty(conn, table):
    """
    Description: data quality function, check if postgres table is empty
    Arguments:
        conn: psycopg2 connection to postgres
        table: table name to load into
    Returns:
        string - with table is empty or not
    """
    cursor = conn.cursor()
    sql = f"SELECT COUNT(*) FROM {table}"
    print (sql)
    query = cursor.execute(sql)
    res = cursor.fetchall()[0][0]
    if res > 0:
        return f"table {table} is not empty, count: {res}"
    else:
        return f"table {table} is empty"


def create_time_table(conn, table):
    """
    Description: Create time table from trips table 
    Arguments:
        conn: a connection to postgres db
        table: the name of the table to be created
    Returns:
        None
    """
    
    query = f"""
    BEGIN;
    DROP TABLE IF EXISTS {table};
    
    select 
        trip_id,
        EXTRACT(YEAR from pickup_datetime) as pickup_year,
        EXTRACT(MONTH from pickup_datetime) as pickup_month,
        EXTRACT(DAY from pickup_datetime) as pickup_day,
        EXTRACT(HOUR from pickup_datetime) as pickup_hour,
        EXTRACT(MINUTE from pickup_datetime) as pickup_minute,
        EXTRACT(YEAR from dropoff_datetime) as dropoff_year,
        EXTRACT(MONTH from dropoff_datetime) as dropoff_month,
        EXTRACT(DAY from dropoff_datetime) as dropoff_day,
        EXTRACT(HOUR from dropoff_datetime) as dropoff_hour,
        EXTRACT(MINUTE from dropoff_datetime) as dropoff_minute
    into 
        {table}
    from 
        staging.trips;
        
    COMMIT;
    """
    cursor = conn.cursor()
    
    print("Create time table for trips table")
    cursor.execute(query)
    
    conn.commit()
    print("Create table completed...")


def geom_from_wtl(conn, table, wkt_col, geom_col, epsg):
    """
    Description: Create geom column from wkt in postgres table 
    Arguments:
        conn: a connection to postgres db
        table: table to create new geom column 
        wkt_col: name of column who have wkt info
        geom_col: name for new columm where geom will be created
        epsg: system refernce code
    Returns:
        None
    """
    
    query = f"""
    ALTER TABLE {table} 
    DROP COLUMN IF EXISTS {geom_col};
    
    ALTER TABLE {table} ADD COLUMN {geom_col} GEOMETRY;

    UPDATE {table}
    SET geom = GeomFromText(({wkt_col}), {epsg});
    """
    
    print(query)
    cursor = conn.cursor()
    
    print("Creating geom field")
    cursor.execute(query)
    
    conn.commit()
    print("completed...")
    

def geom_from_lat_lon(conn, table, lon_col, lat_col, geom_col, epsg):
    """
    Description: Create geom column from lat and lon columns in postgres table 
    Arguments:
        conn: a connection to postgres db
        table: table to create new geom column 
        lon_col: name of column who have lat coordiantes
        lat_col: name of column who have lon coordiantes
        epsg: system refernce code
    Returns:
        None
    """
    
    query = f"""
    ALTER TABLE {table} 
    DROP COLUMN IF EXISTS {geom_col};
    
    ALTER TABLE {table} ADD COLUMN {geom_col} GEOMETRY;

    UPDATE {table}
    SET {geom_col} = GeomFromText(CONCAT('POINT(' ,{lon_col},' ', {lat_col}, ')'), {epsg});
    """
    
    print(query)
    cursor = conn.cursor()
    
    print("Creating geom field")
    cursor.execute(query)
    
    conn.commit()
    print("completed...")


def fix_geom(conn, table, geom_field):
    """
    Description: data quality function, check for invalid geom and try to fix them
    Arguments:
        conn: psycopg2 connection to postgres
        table: table name to check
        geom_field: geom field name
    Returns:
        None
    """
    
    count_query=f"""
    select count(*) from {table} where st_isvalid({geom_field})=False;
    """
    fix_query=f"""
    update {table} 
    set {geom_field} = ST_MakeValid({geom_field}) 
    where st_isvalid({geom_field})=False;
    """

    cursor = conn.cursor()

    query = cursor.execute(count_query)
    res = cursor.fetchall()[0][0]
    if res > 0:
        print(f"Fixing table {table}")
        cursor.execute(count_query)
    else:
        print(f"{table} does not have invalid geom")
    
    
    conn.commit()
    print("completed...")

def spatial_join(conn, schema, table, geom_point, new_table_name, fields, grid_pk_alias, zone_pk_alias):
    """
    Description: Join and create new table using spatial join
    Arguments:
        conn: a connection to postgres db
        schema: the schema to save and read the new table
        table: point data table
        geom_point: geom field for point data table
        new_table_name: the name for the new table
        fields: the fields the user want to save from the point data table
        grid_pk_alias: the name for grid pk in the created table
        zone_pk_alias: the name for taxi service zone pk in the created table
    Returns:
        None
    """
    
    fields_str = " "
    tmp_lst = [f"{schema}.{table}.{x}," for x in fields]
    tmp_lst.append(f"{schema}.grid_250m.grid_id AS {grid_pk_alias}")
    fields_str = (fields_str.join(tmp_lst))
    
    query=f"""    
    DROP TABLE IF EXISTS staging.{new_table_name};
    
    create table {schema}.{new_table_name} as with cte_sj as (
    select
        {fields_str}
    from
        {schema}.{table}
    join {schema}.grid_250m on
        ST_Contains({schema}.grid_250m.geom, {schema}.{table}.{geom_point}) )

    select
        cte_sj.*, {schema}.taxi_zones.taxi_zone_id AS {zone_pk_alias}
    from 
        cte_sj
    join {schema}.taxi_zones on
        ST_Contains({schema}.taxi_zones.geom, cte_sj.{geom_point});
    """
    
    cursor = conn.cursor()
    
    print("Spatial Join tables")
    print("Creating new table, may take a while")
    cursor.execute(query)
    
    conn.commit()
    print("Create table completed...")


def insert_into_table(conn, old_table, new_table, truncate=True):
    """
    Description: Create geom column from lat and lon columns in postgres table 
    Arguments:
        conn: a connection to postgres db
        old_table: from where to select the data
        new_table: where to insert the data
        truncate: clear the table if true
    Returns:
        None
    """
    
    cursor = conn.cursor()

    query = f"""
    INSERT INTO {new_table}
    SELECT * FROM {old_table}
    """
    
    if truncate:
        trucate_query  = f"TRUNCATE TABLE {new_table};"
        cursor.execute(trucate_query)

    print(query)
    
    print("Insert data to new table")
    cursor.execute(query)
    
    conn.commit()
    print("completed...")


if __name__ == '__main__':
    # get files path and param for psotgres
    param_dic = get_param("../finalData//config.cfg") 
    loc = get_files_path("../finalData/config.cfg")

    # Create connection for Postgres
    conn = connect(param_dic)

    # Read csv/json files
    trips_path = loc.get("trips")
    census_block_loc_path = loc.get("census_block_loc")
    census_tracts_path = loc.get("census_tracts")
    nyc_grid_path = loc.get("nyc_grid")
    taxi_zones_path = loc.get("taxi_zones")

    trips = read_file(trips_path, "csv", "trip_id")
    census_block_loc = read_file(census_block_loc_path, "csv", "census_block_id")
    census_tracts = read_file(census_tracts_path, "csv", "census_temp_id", False)
    nyc_grid = read_file(nyc_grid_path, "json", "grid_id")
    taxi_zones = read_file(taxi_zones_path, "csv", "taxi_zone_id")

    # prepare census dataset
    census_data = merge_census(census_tracts, census_block_loc)

    # load the data
    execute_values(conn, trips, "staging.trips")
    execute_values(conn, taxi_zones, "staging.taxi_zones")
    execute_values(conn, nyc_grid, "staging.grid_250m")
    execute_values(conn, census_data, "staging.census")

    # check not empty
    check_empty(conn, "staging.taxi_zones")
    check_empty(conn, "staging.grid_250m")
    check_empty(conn, "staging.trips")
    check_empty(conn, "staging.census")

    # create time table for trips and load it
    create_time_table(conn, "staging.trips_datetime")
    check_empty(conn, "staging.trips_datetime") 

    # Create geometry
    geom_from_wtl(conn,'staging.taxi_zones', 'wkt', 'geom', '4326')
    geom_from_wtl(conn,'staging.grid_250m', 'wkt', 'geom', '4326')
    geom_from_lat_lon(conn, 'staging.census', 'longitude', 'latitude', 'geom', '4326')
    geom_from_lat_lon(conn, 'staging.trips', 'pickup_longitude', 'pickup_latitude', 'pickup_geom', '4326')
    geom_from_lat_lon(conn, 'staging.trips', 'dropoff_longitude', 'dropoff_latitude', 'dropoff_geom', '4326')

    # check for bad geomtry - if true -> fix
    # important for spatial join
    fix_geom(conn, "staging.grid_250m", "geom")
    fix_geom(conn, "staging.taxi_zones", "geom")
    fix_geom(conn, "staging.trips", "dropoff_geom")
    fix_geom(conn, "staging.census", "geom")

    # Spatial join
    census_fields = ["totalpop", "men", "women", "hispanic", "white", "black", "native", "asian", "citizen", "income", "poverty",
    "childpoverty", "professional", "service", "office", "construction", "production", "drive", "carpool", "transit", "walk",
    "othertransp", "workathome", "meancommute", "employed", "privatework", "publicwork", "selfemployed", "familywork",
    "unemployment", "census_id", "geom"]
    trips_fields = ["passenger_count", "trip_duration", "trip_id", "pickup_geom", "dropoff_geom"]
    pickup_fields = ["*"]

    spatial_join(conn, "staging", "census", "geom", "census_sj", census_fields, "grid_id", "taxi_zone_id")
    spatial_join(conn, "staging", "trips", "pickup_geom", "pickup_sj", trips_fields, "pickup_grid_id", "pickup_taxi_zone_id")
    spatial_join(conn, "staging", "pickup_sj", "dropoff_geom", "trips_sj", pickup_fields, "dropoff_grid_id", "dropoff_taxi_zone_id")

    # check for empty tables
    check_empty(conn, "staging.trips_sj")
    check_empty(conn, "staging.census_sj")

    # loading the data to production
    insert_into_table(conn, "staging.census_sj", "data.nyc_census")
    insert_into_table(conn, "staging.grid_250m", "data.nyc_grid_250m")
    insert_into_table(conn, "staging.trips_sj", "data.nyc_taxi_trips")
    insert_into_table(conn, "staging.taxi_zones", "data.nyc_taxi_zones")
    insert_into_table(conn, "staging.trips_datetime", "data.nyc_trips_datetime")

    # check not empty
    check_empty(conn, "data.nyc_trips_datetime")
    check_empty(conn, "data.nyc_taxi_zones")
    check_empty(conn, "data.nyc_taxi_trips")
    check_empty(conn, "data.nyc_grid_250m")
    check_empty(conn, "data.nyc_census")

    print("Done")
