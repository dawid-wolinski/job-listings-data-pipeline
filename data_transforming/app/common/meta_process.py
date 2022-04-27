from datetime import datetime
import pandas as pd
from .s3 import S3BucketConnector
from .custom_exceptions import WrongMetafileException
import collections
from logging import Logger


class MetaProcess():

    @staticmethod
    def update_meta_file(bucket: S3BucketConnector, logger: Logger, metafile_key: str, transformed_files: list) -> None:
        df_new = pd.DataFrame(columns=['file_name', 'datetime_of_processing'])
        df_new['file_name'] = transformed_files
        df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        try:
            df_old = bucket.read_s3_to_df(metafile_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                logger.error('Columns in metafile do not match columns in new dataframe.')
                raise WrongMetafileException
            df_all = pd.concat([df_old, df_new])
        except bucket.session.client('s3').exceptions.NoSuchKey:
            df_all = df_new
        bucket.write_df_to_s3(df_all, metafile_key)


    @staticmethod
    def get_files_to_transform(bucket: S3BucketConnector, logger: Logger, metafile_key: str, data_files_source_path: str) -> list:
        try:
            meta_df = bucket.read_s3_to_df(metafile_key)
            meta_names = set(meta_df['file_name'])
        except bucket.session.client('s3').exceptions.NoSuchKey:
            meta_names = set()
        
        bucket_objects = bucket.get_prefix_files(data_files_source_path)
        all_file_names = {obj.key for obj in bucket_objects}
        
        
        files_to_transform = all_file_names - meta_names
        files_to_transform = list(files_to_transform)
        #files_to_transform.sort(reverse=False)
        
        return files_to_transform