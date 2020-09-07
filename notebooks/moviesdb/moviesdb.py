import csv
import gzip
import pandas as pd
from datetime import datetime


class MoviesDb(object):
    
    
    def __init__(self):
        import sqlite3
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
        
        
    def write_row_to_table(self, table_name, row_dict):
        query = f"""
            INSERT INTO {table_name} ({', '.join(row_dict.keys())})
            VALUES {tuple(row_dict.values())};
        """
        self.cur.execute(query)
        
        
class _TitleBasicsParser(MoviesDb):
    
    
    def __init__(self):
        super().__init__()
        self.file_path = '../zippedData/imdb.title.basics.csv.gz'
    
    
    def create_table_titles(self):
        table_name = 'titles'
        columns_string = """(
            tconst TEXT PRIMARY KEY,
            primary_title TEXT,
            start_year INTEGER
            )
        """
        self.create_table(table_name, columns_string)
    
    
    def create_table_runtimes(self):
        table_name = 'runtimes'
        columns_string = """(
            tconst TEXT PRIMARY KEY,
            runtime_minutes INTEGER,
            FOREIGN KEY(tconst) REFERENCES titles(tconst)
            )
        """
        self.create_table(table_name, columns_string)
     
    
    def create_table_genres(self):
        table_name = 'genres'
        columns_string = """(
            tconst TEXT PRIMARY KEY,
            Action INTEGER DEFAULT 0,
            Adult INTEGER DEFAULT 0,
            Adventure INTEGER DEFAULT 0,
            Animation INTEGER DEFAULT 0,
            Biography INTEGER DEFAULT 0,
            Comedy INTEGER DEFAULT 0,
            Crime INTEGER DEFAULT 0,
            Documentary INTEGER DEFAULT 0,
            Drama INTEGER DEFAULT 0,
            Family INTEGER DEFAULT 0,
            Fantasy INTEGER DEFAULT 0,
            Game_Show INTEGER DEFAULT 0,
            History INTEGER DEFAULT 0,
            Horror INTEGER DEFAULT 0,
            Music INTEGER DEFAULT 0,
            Musical INTEGER DEFAULT 0,
            Mystery INTEGER DEFAULT 0,
            News INTEGER DEFAULT 0,
            Reality_TV INTEGER DEFAULT 0,
            Romance INTEGER DEFAULT 0,
            Sci_Fi INTEGER DEFAULT 0,
            Short INTEGER DEFAULT 0,
            Sport INTEGER DEFAULT 0,
            Talk_Show INTEGER DEFAULT 0,
            Thriller INTEGER DEFAULT 0,
            War INTEGER DEFAULT 0,
            Western INTEGER DEFAULT 0,
            FOREIGN KEY(tconst) REFERENCES titles(tconst)
            )
        """
        self.create_table(table_name, columns_string)
        
        
    def get_row_dict_title(self, row):
        row_dict_title = {'tconst': row['tconst'],
                          'primary_title': row['primary_title'],
                          'start_year': int(row['start_year'])
                         }
        return row_dict_title


    def get_row_dict_runtime(self, row):
        row_dict_runtime = {'tconst': row['tconst'],
                            'runtime_minutes': int(row['runtime_minutes'])
                           }
        return row_dict_runtime


    def get_row_dict_genre(self, row):
        row_dict_genre = dict()
        row_dict_genre['tconst'] = row['tconst']
        for genre in row['genres'].split(','):
            clean_genre = genre.replace('-', '_')
            row_dict_genre[clean_genre] = 1
        return row_dict_genre


    def import_row(self, row):
        row_dict_title = self.get_row_dict_title(row)
        self.write_row_to_table('titles', row_dict_title)
        if len(row['runtime_minutes']) > 0:
            row_dict_runtime = self.get_row_dict_runtime(row)
            self.write_row_to_table('runtimes', row_dict_runtime)
        if len(row['genres']) > 0:
            row_dict_genre = self.get_row_dict_genre(row)
            self.write_row_to_table('genres', row_dict_genre)


    def import_data_titles(self):
        self.create_table_titles()
        self.create_table_runtimes()
        self.create_table_genres()
        with gzip.open(self.file_path, mode = 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.import_row(row)
        self.conn.commit()
        self.close()

        
class _TitleRatingsParser(MoviesDb):
    
    
    def __init__(self):
        super().__init__()
        self.file_path = '../zippedData/imdb.title.ratings.csv.gz'
    
    
    def create_table_ratings(self):
        table_name = 'ratings'
        columns_string = """(
            tconst TEXT PRIMARY KEY,
            averagerating REAL,
            numvotes INTEGER,
            FOREIGN KEY(tconst) REFERENCES titles(tconst)
            )
        """
        self.create_table(table_name, columns_string)
    
    
    def get_row_dict_rating(self, row):
        row_dict_rating = {'tconst': row['tconst'],
                          'averagerating': float(row['averagerating']),
                          'numvotes': int(row['numvotes'])
                         }
        return row_dict_rating
    
    
    def import_row(self, row):
        row_dict_rating = self.get_row_dict_rating(row)
        self.write_row_to_table('ratings', row_dict_rating)
        
    def import_data_ratings(self):
        self.create_table_ratings()
        with gzip.open(self.file_path, mode = 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.import_row(row)
        self.conn.commit()
        self.close()
        

class _TitleBudgetsParser(MoviesDb):
    
    def __init__(self):
        super().__init__()
        self.file_path = '../zippedData/tn.movie_budgets.csv.gz'
        
        
    def create_table_budgets(self):
        table_name = 'budgets'
        columns_string = """(
            id INTEGER PRIMARY KEY,
            primary_title TEXT,
            start_year INTEGER,
            release_date TEXT,
            production_budget INTEGER,
            domestic_gross INTEGER,
            worldwide_gross INTEGER,
            FOREIGN KEY (primary_title, start_year) REFERENCES titles(primary_title, start_year)
            )
        """
        self.create_table(table_name, columns_string)

        
    def get_row_dict_budget(self, row):
        row_dict_budget = {'primary_title': row['movie'],
                           'start_year': datetime.strptime(row['release_date'], '%b %d, %Y').year,
                           'release_date': row['release_date'],
                           'production_budget': int(row['production_budget'].replace('$','').replace(',','')),
                           'domestic_gross': int(row['domestic_gross'].replace('$','').replace(',','')),
                           'worldwide_gross': int(row['worldwide_gross'].replace('$','').replace(',',''))
                          }
        return row_dict_budget
                           
        
    def import_row(self, row):
        row_dict_budget = self.get_row_dict_budget(row)
        self.write_row_to_table('budgets', row_dict_budget)
    
    
    def import_data_budgets(self):
        self.create_table_budgets()
        with gzip.open(self.file_path, mode = 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.import_row(row)
        self.conn.commit()
        self.close()
    
    
class _NameBasicsParser(MoviesDb):
    
    def __init__(self):
        super().__init__()
        self.file_path = '../zippedData/imdb.name.basics.csv.gz'
    
    
    def create_table_names(self):
        table_name = 'names'
        columns_string = """(
            nconst TEXT PRIMARY KEY,
            primary_name TEXT
            )
        """
        self.create_table(table_name, columns_string)
        
       
    def get_row_dict_name(self,row):
        row_dict_name = {'nconst': row['nconst'],
                         'primary_name': row['primary_name']
                        }
        return row_dict_name
    
    
    def import_row(self, row):
        row_dict_name = self.get_row_dict_name(row)
        self.write_row_to_table('names', row_dict_name)
    
    def import_data_names(self):
        self.create_table_names()
        with gzip.open(self.file_path, mode = 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.import_row(row)
        self.conn.commit()
        self.close()

        
class _PrincipalsParser(MoviesDb):
    
    
    def __init__(self):
        super().__init__()
        self.file_path = '../zippedData/imdb.title.principals.csv.gz'
    
    
    def create_table_principals(self):
        table_name = 'principals'
        columns_string = """(
            tconst TEXT,
            ordering INTEGER,
            nconst TEXT,
            category TEXT,
            FOREIGN KEY('tconst') REFERENCES titles('tconst'),
            FOREIGN KEY('nconst') REFERENCES names('nconst')
            )
        """
        self.create_table(table_name, columns_string)
    
    def get_row_dict_principal(self, row):
        row_dict_principal = {'tconst' : row['tconst'],
                              'ordering': int(row['ordering']),
                              'nconst': row['nconst'],
                              'category': row['category']
                             }
        return row_dict_principal
    
    
    def import_row(self, row):
        row_dict_principal = self.get_row_dict_principal(row)
        self.write_row_to_table('principals', row_dict_principal)
    
    
    def import_data_principals(self):
        self.create_table_principals()
        with gzip.open(self.file_path, mode = 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.import_row(row)
        self.conn.commit()
        self.close()
