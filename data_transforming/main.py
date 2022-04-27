from app.common.s3 import S3BucketConnector
from app.common.redshift import RedshiftConnector
from app.common.custom_exceptions import CustomException
from app.transforming.transform import Transformer
from app.transforming.dwh_tool import DataWarehouseTool
from app.common.meta_process import MetaProcess
import yaml
import logging
import logging.config


def run():
    
    config_path = './configs/data-transforming-config.yml'
    config = yaml.safe_load(open(config_path))

    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info('Data transformation started.')
    

    try:
        s3_config = config['s3']
        redshift_config = config['redshift']
        transformer_config = config['transformer']
        dwh_tool_config = config['data_warehouse']
        meta_config = config['meta']

        bucket_connector = S3BucketConnector(**s3_config)
        redshift_connector = RedshiftConnector(**redshift_config)

        files = MetaProcess.get_files_to_transform(bucket_connector, logger, **meta_config)
        transformer = Transformer(bucket=bucket_connector, files_to_transform=files, **transformer_config)
        dwh_tool = DataWarehouseTool(redshift=redshift_connector, bucket=bucket_connector, 
                                    transformed_files=files, metafile_key=meta_config['metafile_key'],
                                    **dwh_tool_config)
        

        df = transformer.get_transformed_data()
        dwh_tool.generate_facts_and_dims(df)


    except CustomException:
        logger.error('Due to raised error the program will be terminated.')
    except Exception:
        logger.exception('An unexpected error has occured. The program will be terminated.')
    

if __name__ == '__main__':
    run()