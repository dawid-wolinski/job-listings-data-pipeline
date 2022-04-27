from datetime import datetime
import pandas as pd
from datetime import date, timedelta
from .s3 import S3BucketConnector
from .custom_exceptions import WrongDateException, WrongMetafileException
import collections
from logging import Logger


class MetaProcess():

    @staticmethod
    def update_meta_file(bucket: S3BucketConnector, logger: Logger, meta_key: str, scrape_dates: list) -> None:
        df_new = pd.DataFrame(columns=['source_date', 'datetime_of_processing'])
        df_new['source_date'] = scrape_dates
        df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        try:
            df_old = bucket.read_s3_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                logger.error('Columns in metafile do not match columns in new dataframe.')
                raise WrongMetafileException
            df_all = pd.concat([df_old, df_new])
        except bucket.session.client('s3').exceptions.NoSuchKey:
            df_all = df_new
        bucket.write_df_to_s3(df_all, meta_key)


    @staticmethod
    def get_dates(start_date: date, end_date: date, bucket: S3BucketConnector, logger: Logger, meta_key: str) -> list:
        try:
            meta_df = bucket.read_s3_to_df(meta_key)
            meta_dates = set(pd.to_datetime(meta_df['source_date']).dt.date)
        except bucket.session.client('s3').exceptions.NoSuchKey:
            meta_dates = set()
        
        if end_date <= start_date:
            logger.error(f'End date ({end_date}) cannot be less than or equal to start date ({start_date}).')
            raise WrongDateException
        delta = (end_date - start_date).days
        dates = {start_date+timedelta(days=day) for day in range(delta)}
        scrape_dates = dates - meta_dates
        scrape_dates = list(scrape_dates)
        scrape_dates.sort(reverse=True)
        if len(scrape_dates) < 1:
            logger.info(f'All jobs published between {start_date} and {end_date} have already been scraped. If you wish to run the script for this period, please remove appropriate dates from the metafile.')
        return scrape_dates