from ..common.redshift import RedshiftConnector
from ..common.s3 import S3BucketConnector
from ..common.meta_process import MetaProcess
import pandas as pd
import logging


# Creates fact, bridge and dimension tables based on the transformed data and loads it to Redshift
class DataWarehouseTool():
    def __init__(self, redshift: RedshiftConnector, bucket: S3BucketConnector,
                transformed_files: str, metafile_key: str,
                fact_table_name: str, fact_table_key: str, 
                one_to_many_template: list, many_to_many_template: list, 
                one_to_many_dims: list, many_to_many_dims: list) -> None:
        self.logger = logging.getLogger(__name__)
        self.redshift = redshift
        self.bucket = bucket
        self.transformed_files = transformed_files
        self.metafile_key = metafile_key
        self.fact_table_name = fact_table_name
        self.fact_table_key = fact_table_key

        # Adds keys to elements in each list (of lists) according to the templates
        self.one_to_many = [dict(zip(one_to_many_template, dim)) for dim in one_to_many_dims]
        self.many_to_many = [dict(zip(many_to_many_template, dim)) for dim in many_to_many_dims]
    

    # Main function
    def generate_facts_and_dims(self, df: pd.DataFrame) -> None:
        if df is None:
            return
        elif df.empty:
            self.logger.warning('The transformed dataframe is empty. No data was loaded to the database.')
            return

        self.logger.info('Creating fact and dimension tables.')
        new_dims_data = {}
        s3_export_tables = []

        # Every keys_list contains name and keys of specific dimension
        for keys_list in self.one_to_many:
            new_df, dim = self.get_fact_dim(df, keys_list['dim_table'], keys_list['natural_key'], keys_list['surrogate_key'])
            df = new_df.copy()
            if not dim.empty: 
                s3_export_tables.append([dim, keys_list['dim_table']])
                self.redshift.write_dataframe(dim, keys_list['dim_table'])
                new_dims_data[keys_list['dim_table']] = len(dim.index)
        

        # For many-to-many dimension there is additionally name and group key of bridge table
        for keys_list in self.many_to_many:
            new_df, dim, br = self.get_fact_dim_br(df, keys_list['dim_table'], keys_list['br_table'], keys_list['natural_key'], 
                                        keys_list['surrogate_key'], keys_list['group_key'])
            df = new_df.copy()
            if not dim.empty: 
                s3_export_tables.append([dim, keys_list['dim_table']])
                self.redshift.write_dataframe(dim, keys_list['dim_table'])
                new_dims_data[keys_list['dim_table']] = len(dim.index)
            if not br.empty: 
                s3_export_tables.append([br, keys_list['br_table']])
                self.redshift.write_dataframe(br, keys_list['br_table'])
                new_dims_data[keys_list['br_table']] = len(br.index)
        

        df = self.set_fact_columns(df)
        s3_export_tables.append([df, 'fact_salary'])
        self.redshift.write_dataframe(df, 'fact_salary')

        
        # Load new tables to S3 and Redshift and update metafile
        for table in s3_export_tables:
            self.bucket.write_df_to_s3(table[0], table[1], True)
        self.redshift.commit()
        MetaProcess.update_meta_file(self.bucket, self.logger, self.metafile_key, self.transformed_files)


        message = f'Job finished. {len(df.index)} new rows were loaded to the fact table.'
        if new_dims_data:
            message += f' Additionally new rows to the following dimension/bridge tables were loaded: {str(new_dims_data)[1:-1]}.'
        self.logger.info(message)
    
    
    # Returns dimnesion table with new values (if any) and df with foreign keys to the dimension
    def get_fact_dim(self, df, source_dim_name, natural_key: str, surrogate_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        source_dim = self.redshift.get_source_table(source_dim_name)
        current_dim = self.create_new_dim(df, source_dim, natural_key, surrogate_key)
        fact = pd.merge(df, current_dim[[natural_key, surrogate_key]], on=natural_key,how='left')
        dim = self.get_table_with_new_values(current_dim, source_dim, surrogate_key)

        return fact, dim


    # Returns dimension and bridge tables with new values (if any) and df with foreign (group) keys to the bridge
    def get_fact_dim_br(self, df: pd.DataFrame, dim_table_name: str, br_table_name: str,
                        natural_key: str, surrogate_key: str, group_key: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        source_dim = self.redshift.get_source_table(dim_table_name)
        source_br = self.redshift.get_source_table(br_table_name)

        # Dimension table based on the df data
        current_dim = self.create_new_dim(df, source_dim, natural_key, surrogate_key, change_lists_into_rows=True)

        # Adds natural keys to bridge table (in order to join with df)
        br_nat_keys = pd.merge(source_br, source_dim[[surrogate_key, natural_key]], on=surrogate_key, how='left')
        br_nat_keys_joined = self.rows_into_lists(br_nat_keys, group_key, natural_key)

        # Creates bridge with new group keys and natural keys. In this case group keys are surrogate keys.
        nat_keys_group_keys = self.create_new_dim(df, br_nat_keys_joined, natural_key, group_key)

        # Fact with appended group keys and dimension with new values
        fact = pd.merge(df, nat_keys_group_keys[[natural_key, group_key]], on=natural_key,how='left')
        dim = self.get_table_with_new_values(current_dim, source_dim, surrogate_key)
        # Bridge with new values
        nat_keys_group_keys_separate = self.lists_into_rows(nat_keys_group_keys, natural_key)
        all_keys = pd.merge(nat_keys_group_keys_separate, current_dim[[natural_key, surrogate_key]], on=natural_key, how='left')
        br = self.get_table_with_new_values(all_keys[[group_key, surrogate_key]], source_dim, surrogate_key)
        br = br.convert_dtypes()
        
        return fact, dim, br


    # Creates dimension table based on unique values from df 
    # with assigned surrogate keys (from source dimension table and new ones - if any)
    def create_new_dim(self, df: pd.DataFrame, source_dim: pd.DataFrame, natural_key: str, surrogate_key: str, change_lists_into_rows: bool=False) -> pd.DataFrame:
        columns = [x for x in source_dim.columns if x in df.columns]
        new_dim = df[columns].copy()
        if change_lists_into_rows:
            new_dim = self.lists_into_rows(new_dim, natural_key)
        new_dim = new_dim.drop_duplicates(natural_key)
        new_dim = new_dim.reset_index(drop=True)
        new_dim = self.assign_surrogate_keys(new_dim, source_dim, natural_key, surrogate_key)
        
        return new_dim


    def assign_surrogate_keys(self, new_dim: pd.DataFrame, source_dim: pd.DataFrame, natural_key: str, surrogate_key: str) -> pd.DataFrame:
        if natural_key == 'published_date':
            source_dim['published_date'] = pd.to_datetime(source_dim['published_date'])

        # Joins new table with source table - only new table values and mutual values
        merged = pd.merge(source_dim[[surrogate_key, natural_key]], new_dim, on=natural_key, how='right')
        source_dim.sort_values(by=[surrogate_key], inplace=True, ascending=False)
        
        # Finds last surrogate key from the source table
        keys = source_dim[surrogate_key].head(1).values
        if keys.size > 0:
            last_key = keys[0]
        else:   # If source table is empty
            last_key = 0
        
        # Assigns surrogate keys to rows without it
        s = (merged[surrogate_key].isna().cumsum() + last_key)
        merged[surrogate_key] = merged[surrogate_key].fillna(s)
        merged[surrogate_key] = merged[surrogate_key].astype(int)
        
        return merged


    # Concatenates values of a specific column based on a key into one list (without brackets [])
    def rows_into_lists(self, table: pd.DataFrame, group_key: str, column: str) -> pd.DataFrame:
        new_table = table.copy()
        new_table = new_table.groupby([group_key], as_index=False).agg({column: lambda x: x.tolist()})
        new_table[column] = new_table[column].str.join(', ')
        
        return new_table


    # Splits string values separated by comma into multiple rows
    def lists_into_rows(self, table: pd.DataFrame, column: str) -> pd.DataFrame:
        new_table = table.copy()
        new_table[column] = new_table[column].str.split(', ')
        new_table[column] = new_table[column].apply(lambda x: sorted(x))
        new_table = new_table.explode(column)
        
        return new_table


    # Creates table with new values - only those present in new table and not source table
    def get_table_with_new_values(self, new_table, source_table, connecting_key: str) -> pd.DataFrame:
        merged = pd.merge(new_table, source_table[connecting_key], on=connecting_key, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
        merged.reset_index(drop=True, inplace=True)
        merged[connecting_key] = merged[connecting_key].astype(int)
        
        return merged

    
    
    def set_fact_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Adds column with new fact keys (based on the last key from source fact table)
        fact_columns = self.redshift.get_source_table_columns(self.fact_table_name)
        last_fact_key = self.redshift.get_source_table_last_key(self.fact_table_name, self.fact_table_key)
        df[fact_columns[0]] = df.index + 1 + last_fact_key
        
        # Changes column order to removes unncecessary ones to match the source fact table
        df = df[fact_columns]

        return df