import os
import csv
import sqlite3
import gzip
import numpy as np
import pandas as pd
from datetime import datetime

class MoviesDb(object):
    
    def __init__(self):
        self.conn = sqlite3.connect('moviesdb/movies.sqlite')
        self.cur = self.conn.cursor()
 

    def close(self):
        self.cur.close()
        self.conn.close()

    
    def list_tables(self):
        query = """
            SELECT name 
            FROM sqlite_master
            WHERE type='table';
        """
        response = self.cur.execute(query).fetchall()
        table_names = [r[0] for r in response]
        return table_names
    

    def list_column_names(self, table_name):
        query = f"""
            PRAGMA table_info({table_name});
            """
        response = self.cur.execute(query).fetchall()
        column_names = [r[1] for r in response]
        return column_names
    

    def load_query_as_df(self, query):
        df = pd.read_sql(query, self.conn)
        return df

    def load_table_as_df(self, table_name):
        query = f"""
            SELECT *
            FROM {table_name};
            """
        df = self.load_query_as_df(query)
        return df


    def create_table(self, table_name, columns_string):
        drop_query = f"""
        DROP TABLE IF EXISTS {table_name};
        """
        create_query = f"""
        CREATE TABLE {table_name} {columns_string};
        """
        self.cur.execute(drop_query)
        self.cur.execute(create_query)