from os import getenv
from dotenv import load_dotenv
from pandas import DataFrame
from geopandas import GeoDataFrame
from sqlalchemy import create_engine, text

class Database():
    def __init__(self):
        # load the .env file variables 
        load_dotenv()

        # get the varaibles 
        self.__db_name = getenv("POSTGRES_DB")
        self.__host = getenv("POSTGRES_HOST")
        self.__username = getenv("POSTGRES_USER")
        self.__port = getenv("POSTGRES_HOST_PORT")
        self.__password = getenv("POSTGRES_PASSWORD")

        # establish connection
        self.connection = create_engine(self.__get_engine_url__())
        print(f"Connection Established!!!\n\t{self.connection}")

    def __get_engine_url__(self):
        return f"postgresql://{self.__username}:{self.__password}@{self.__host}:{self.__port}/{self.__db_name}"
    
    def get_connection(self):
        return self.connection

    def send_df_to_db(
        self, 
        df:DataFrame,
        table_name:str,
        if_exists:str = 'replace',
        index:bool = False,
        dtypes:dict = None
    ):
        df.to_sql(
            name = table_name, 
            con = self.connection, 
            if_exists = if_exists, 
            index = index, 
            dtype = dtypes
        )
        print(f"Loaded data into table '{table_name}'")
    
    def send_gdf_to_db(
        self, 
        gdf:GeoDataFrame,
        table_name:str,
        if_exists:str = 'replace',
        index:bool = False,
        dtypes:dict = None
    ):
        gdf.to_postgis(
            name = table_name, 
            con = self.connection, 
            if_exists = if_exists, 
            index = index, 
            dtype = dtypes
        )

    def execute_sql(self, statement:str):
        with self.connection.connect() as con:
            print(f"Execution started --> {statement}")
            con.execute(
                text(
                    statement
                )
            )
            con.commit()
            print(f"Exectution completed --> {statement}")
