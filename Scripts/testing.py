import configparser
import numpy
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import sys
from etl import *

testing_points = read_file("../finalData/testingPoints.csv", "csv", "test_id")
testing_grid = read_file("../finalData/nyc_grid_250m.json", "json", "grid_id")
testing_zones = read_file("../finalData/nyc_taxi_lookup_zones.csv", "csv", "taxi_zone_id")


def test_points_size(df):
    df_length=df.shape[0]
    assert df_length==26300, "should be 26300"

def test_points_col(df, col_list):
    cols=list(df.columns)
    assert cols==col_list, "should be ['id', 'wkt', 'timestamp', 'test_id']"

def test_execute_values(conn, table, count):
    cursor = conn.cursor()
    sql = f"SELECT COUNT(*) FROM {table}"
    query = cursor.execute(sql)
    res = cursor.fetchall()[0][0]
    assert res==26300, f"should be {count}, but {res}"

def test_geom_from_wtl(conn, table="public.test_points", geom_field="geom"):
    sql=f"select count(*) from {table} where st_isvalid({geom_field})=False;"
    cursor = conn.cursor()
    query = cursor.execute(sql)
    res = cursor.fetchall()[0][0]
    assert res==0, f"should be {0}, but {res}"

def test_time_table(conn, table, test_col, min_value, max_value):
    sql=f"SELECT count(*) FROM {table} WHERE {test_col} NOT BETWEEN {min_value} and {max_value}"
    cursor = conn.cursor()
    query = cursor.execute(sql)
    res = cursor.fetchall()[0][0]
    assert res==0, f"should be {0}, but {res}"

if __name__ == '__main__':
   test_points_size(testing_points)
   col_list=['id', 'wkt', 'timestamp', 'test_id']
   test_points_col
   print("Read file function passed")
   
   param_dic = get_param("../finalData//config.cfg") 
   conn = connect(param_dic)
   
   execute_values(conn, testing_points, "public.test_points", truncate=True)
   test_execute_values(conn, "public.test_points", 26300)
   print("execute_values function passed")

   geom_from_wtl(conn, "public.test_points", "wkt", "geom", 4326)
   test_geom_from_wtl(conn)
   print("all rows have valid geometry, create geom function passed")
   
   create_time_table(conn, "datetime_table", "timestamp", "timestamp", "test_id", "public.test_points")
   test_execute_values(conn, "public.datetime_table", 26300)
   test_time_table(conn, "public.datetime_table", "pickup_year", 1990, 2020)
   print("create_time_table function passed")