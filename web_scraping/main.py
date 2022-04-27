from app.common.s3 import S3BucketConnector
from app.web_scraping.scraper import WebScraper
from app.common.custom_exceptions import CustomException
import yaml
import logging
import logging.config


def run():
    
    config_path = './configs/web-scraping-config.yml'
    config = yaml.safe_load(open(config_path))

    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info('Scraping started.')

    try:
        s3_config = config['s3']
        scraper_config = config['scraper']
        meta_config = config['meta']

        bucket_connector = S3BucketConnector(**s3_config)
        scraper = WebScraper(target_bucket=bucket_connector, **scraper_config, **meta_config)

        scraper.scrape()
    except CustomException:
        logger.error('Due to raised error the program will be terminated.')
    except Exception:
        logger.exception('An unexpected error has occured. The program will be terminated.')
    scraper.driver.quit()
    

if __name__ == '__main__':
    run()