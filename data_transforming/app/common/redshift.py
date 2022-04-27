import redshift_connector
from dotenv import load_dotenv
import os
import pandas as pd


class RedshiftConnector():
    def __init__(self, host: str, port: str, database: str, user: str, password: str) -> None:
        load_dotenv()

        self.connector = redshift_connector.connect(
            host=host,
            port=int(os.environ[port]),
            database=os.environ[database],
            user=os.environ[user],
            password=os.environ[password])
    

    def get_source_table(self, source_table_name: str) -> pd.DataFrame:
        cursor = self.connector.cursor().execute(f'SELECT * FROM {source_table_name}')
        source_table = cursor.fetch_dataframe()

        if source_table is None:
            column_list = self.get_source_table_columns(source_table_name)
            source_table = pd.DataFrame(columns=column_list)
        
        return source_table
    

    def get_source_table_columns(self, source_table_name: str) -> list:
        query_statement = "SELECT column_name FROM information_schema.columns "\
                          f"WHERE table_name = '{source_table_name}' "\
                          "ORDER by ordinal_position;"
        cursor = self.connector.cursor().execute(query_statement)
        source_table = cursor.fetch_dataframe()
        column_list = source_table['column_name'].values.tolist()

        return column_list


    def get_source_table_last_key(self, source_table_name: str, surrogate_key: str) -> int:
        query_statement = f"SELECT {surrogate_key} FROM {source_table_name} "\
                          f"ORDER BY {surrogate_key} DESC LIMIT 1"
        cursor = self.connector.cursor().execute(query_statement)
        source_table = cursor.fetch_dataframe()

        if source_table is None:
            return 0
        else:
            last_key = source_table.iloc[0, 0]
            return last_key


    def write_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        self.connector.cursor().write_dataframe(df, table_name)


    def commit(self) -> None:
        self.connector.commit()