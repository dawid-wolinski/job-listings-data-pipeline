import boto3
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime
import os
from .custom_exceptions import WrongFileFormat
import logging


class S3BucketConnector():
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str, dwh_tables_target_path: str, target_format: str) -> None:
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key], 
                                    aws_secret_access_key=os.environ[secret_key])
        self.logger = logging.getLogger(__name__)
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket_name)
        self.dwh_tables_target_path = dwh_tables_target_path
        self.target_format = target_format


    def write_df_to_s3(self, df: pd.DataFrame, key: str, dwh_table: bool=False) -> None:
        if dwh_table:
            key = self.generate_file_key(key)

        format_position = key.rfind('.') + 1
        file_format = key[format_position:]
        
        if file_format == 'parquet':
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
        elif file_format == 'csv':
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
        else:
            self.logger.error(f"The file format '{self.target_format}' is not supported to be written to s3.")
            raise WrongFileFormat
        
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)


    def read_s3_to_df(self, key: str, decoding='utf-8', sep=',') -> pd.DataFrame:
        format_position = key.rfind('.') + 1
        file_format = key[format_position:]
        if file_format == 'csv':
            csv_file = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
            data = StringIO(csv_file)
            df = pd.read_csv(data, delimiter=sep)
        elif file_format == 'parquet':
            parquet_file = self._bucket.Object(key=key).get().get('Body').read()
            data = BytesIO(parquet_file)
            df = pd.read_parquet(data)
        else:
            self.logger.error(f"The file format '{file_format}' is not supported to be read from s3.")
            raise WrongFileFormat
            
        return df

    
    def generate_file_key(self, table_name: str) -> str:
        today_date = datetime.today().strftime('%Y%m%d_%H%M%S')
        key = f'{self.dwh_tables_target_path}{table_name}/{table_name}_{today_date}.{self.target_format}'
        return key


    def get_prefix_files(self, prefix: str) -> list:
        return self._bucket.objects.filter(Prefix=prefix)

