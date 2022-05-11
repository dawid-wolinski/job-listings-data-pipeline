import pandas as pd
import numpy as np
from ..common.s3 import S3BucketConnector
import logging
from ..common.custom_exceptions import WrongDataFileException


class Transformer():
    def __init__(self, bucket: S3BucketConnector, files_to_transform: str, 
                s3_transformer_different_column_names: str, 
                transformer_dwh_different_column_names: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.bucket = bucket
        self.files_to_transform = files_to_transform
        self.source_columns_names = s3_transformer_different_column_names
        self.target_columns_names = transformer_dwh_different_column_names
        

    def get_transformed_data(self) -> pd.DataFrame:
        if len(self.files_to_transform) < 1:
             self.logger.info(f'All data files in S3 bucket have already been transformed. If you wish to run the script on specific files, please remove their names from the metafile.')
             return

        df = self.get_data()
        df = self.transform_data(df)

        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        
        # Source column names can be changed to fit the column names used by the transformer
        df.rename(columns=self.source_columns_names, inplace=True)
        
        # offer_id to string
        df['offer_id'] = df['offer_id'].astype(str)
        # Remove leading and trailing whitespaces from data fame
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        # Offers which do not include full-time employment are removed. None work_schedule is considered to be full-time
        df = df[df['work_schedule'].isnull() | df['work_schedule'].str.contains('pełny etat')].copy()
        # Remove offers with no category_id
        df.dropna(subset=['category_id'], inplace=True)
        
        df = self.transform_date(df)
        df = self.transform_employer(df)
        df = self.transform_location(df)
        df = self.transform_salary(df)
        df = self.transform_contract(df)
        df = df.reset_index(drop=True)

        # Column names used by the transformer can be renamed to fit the target database
        df.rename(columns=self.target_columns_names, inplace=True)
        
        return df


    def get_data(self) -> pd.DataFrame:
        frames = [self.bucket.read_s3_to_df(file) for file in self.files_to_transform]
       
        if not all(len(frame.columns) == len(frames[0].columns) for frame in frames):
            self.logger.error('Files cannot be transformed because their data has different number of columns.')
            raise WrongDataFileException
        elif not all([len(frames[0].columns.intersection(df.columns)) == frames[0].shape[1] for df in frames]):
            self.logger.error('Files cannot be transformed because their data has different column names.')
            raise WrongDataFileException
        
        df = pd.concat(frames)

        return df


    def transform_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df['published_date'] = pd.to_datetime(df['published_date'])
        date_column_index = df.columns.get_loc('published_date') + 1
        df.insert(date_column_index, 'day', df['published_date'].dt.day)
        df.insert(date_column_index, 'month', df['published_date'].dt.month)
        df.insert(date_column_index, 'quarter', df['published_date'].dt.quarter)
        df.insert(date_column_index, 'year', df['published_date'].dt.year)
        
        return df


    def transform_employer(self, df: pd.DataFrame) -> pd.DataFrame:
        # remove offers without company info
        no_employer_info = df['employer_name'].isnull() | df['employer_address'].isnull() | df['employer_tax_id'].isnull()
        df = df[~no_employer_info].copy()
        df = df.reset_index(drop=True)
        # clean address and nip
        df['employer_address'] = df['employer_address'].apply(lambda x: x.replace('\n', ', '))
        df['employer_tax_id'] = df['employer_tax_id'].apply(lambda x: x[5:])
        wrong_tax_id = df['employer_tax_id'].str.len() > 10
        df = df[~wrong_tax_id].copy()
        df = df.reset_index(drop=True)

        return df


    def transform_location(self, df: pd.DataFrame, max_locations: int=5) -> pd.DataFrame:
        # If work_mode is only remote/mobile, then location is also only remote/mobile
        df['location'] = np.where(df['work_mode'] == 'praca zdalna', 'praca zdalna', df['location'])
        df['location'] = np.where(df['work_mode'] == 'praca mobilna', 'praca mobilna', df['location'])
        # Keep only city, without street address
        df['location'] = df['location'].str.split(', ')
        df['location'] = df['location'].apply(lambda x:x[-1])
        df['location'] = df['location'].astype(str).str.split('|')
        # If there are more than maximum number of locations
        # if work_mode does not include remote/mobile work, then the row is removed
        condition_delete = (df['location'].str.len() > max_locations) & ~(df['work_mode'].str.contains('praca mobilna') | df['work_mode'].str.contains('praca zdalna'))
        df = df[~condition_delete].copy()
        # if work_mode includes remote/mobile work, then location is equal to remote/mobile work
        condition_remote = (df['location'].str.len() > max_locations) & (df['work_mode'].str.contains('praca zdalna'))
        df['location'] = np.where(condition_remote, 'praca zdalna', df['location'])
        condition_mobile = (df['location'].str.len() > max_locations) & (df['work_mode'].str.contains('praca mobilna'))
        df['location'] = np.where(condition_mobile, 'praca mobilna', df['location'])
        df['location'] = df['location'].str.join(', ')
        # If there are 5 or less locations
        # if work_mode contains remote/mobile work and other modes, then remote/mobile work is treated as additional location
        condition_add_remote = ((df['work_mode'] != 'praca zdalna') & (df['work_mode'] != 'praca mobilna')) & (df['work_mode'].str.contains('praca zdalna'))
        df['location'] = np.where(condition_add_remote, df['location'] + ', praca zdalna', df['location'])
        condition_add_mobile = ((df['work_mode'] != 'praca zdalna') & (df['work_mode'] != 'praca mobilna')) & (df['work_mode'].str.contains('praca mobilna'))
        df['location'] = np.where(condition_add_mobile, df['location'] + ', praca mobilna', df['location'])
        
        return df


    def transform_salary(self, df: pd.DataFrame) -> pd.DataFrame:
        # Remove currency unit form max_salary
        df['max_salary'] = df['max_salary'].map(lambda x: x.rstrip(' zł'))
        # If there is no min_salary then it is equal to max_salary
        df['min_salary'] = np.where(df['min_salary'].isnull(), df['max_salary'], df['min_salary'])
        # Remove '-' from min_salary
        df['min_salary'] = df['min_salary'].map(lambda x: x.rstrip('–'))
        # Remove '\xa0' unicode from salaries
        df['min_salary'] = df['min_salary'].apply(lambda x: x.replace('\xa0', ''))
        df['max_salary'] = df['max_salary'].apply(lambda x: x.replace('\xa0', ''))
        # Convert salaries to numeric type
        df['min_salary'] = df['min_salary'].apply(lambda x: x.replace(',', '.'))
        df['max_salary'] = df['max_salary'].apply(lambda x: x.replace(',', '.'))
        df['min_salary'] = pd.to_numeric(df['min_salary'])
        df['max_salary'] = pd.to_numeric(df['max_salary'])
        # In case someone used monthly salary for hourly (and other way around) the row is removed
        wrong_monthly = (df['min_salary'] < 1000) & (df['salary_type'].str.contains('mies.'))
        df = df[~wrong_monthly].copy()
        wrong_hourly = (df['min_salary'] >= 1000) & (df['salary_type'].str.contains('godz.'))
        df = df[~wrong_hourly].copy()
        # Convert hourly salary to monthly
        df['min_salary'] = np.where(df['salary_type'].str.contains('godz.'), df['min_salary']*8*20, df['min_salary'])
        df['max_salary'] = np.where(df['salary_type'].str.contains('godz.'), df['max_salary']*8*20, df['max_salary'])

        return df


    def transform_contract(self, df: pd.DataFrame) -> pd.DataFrame:
        # Split each contract into separate row
        df['contract_type'] = df['contract_type'].str.split(', ')
        df = df.explode('contract_type')
        
        return df

    