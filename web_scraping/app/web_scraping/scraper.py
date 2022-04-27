import requests
from bs4 import BeautifulSoup, Tag, ResultSet
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from ..common.custom_exceptions import TagNotFoundException
from ..common.s3 import S3BucketConnector
from ..common.meta_process import MetaProcess
from io import StringIO
from datetime import date, datetime
import pandas as pd
import logging
from csv import writer
from contextlib import contextmanager 



class WebScraper():
    def __init__(self, source_link: str, column_headers: list, 
                month_names: dict, target_bucket: S3BucketConnector, metafile_key: str, 
                start_date: str, end_date: str=date.today(), webdriver_path: str='',
                target_file_format: str='parquet', parser: str='lxml', 
                sleep_multiplier: int=2, date_format: str='%Y-%m-%d') -> None:
        self.logger = logging.getLogger(__name__)
        self.chrome_options = self.get_chrome_options()
        if webdriver_path:  # Run locally
            self.driver = webdriver.Chrome(service=Service(webdriver_path), options=self.chrome_options)
        else:               # Run from Docker container
            self.driver = webdriver.Chrome(options=self.chrome_options)
        self.source_link = source_link
        self.month_names = month_names
        self.column_headers = column_headers
        self.parser = parser
        self.target_bucket = target_bucket
        self.target_file_format = target_file_format
        self.start_date = datetime.strptime(start_date, date_format).date()
        if type(end_date) == str:
            end_date = datetime.strptime(end_date, date_format).date()
        self.end_date = end_date
        self.running = False
        self.sleep_multiplier = sleep_multiplier
        self.metafile_key = metafile_key
    
    
    def get_chrome_options(self) -> Options:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        return chrome_options
        
        
    # main function
    def scrape(self) -> None:
        all_data = [self.column_headers]
        current_page = 1
        scrape_dates = MetaProcess.get_dates(self.start_date, self.end_date, self.target_bucket, self.logger, self.metafile_key)
        if len(scrape_dates) > 0: 
            self.running = True

        while self.running == True:
            page_link = f'{self.source_link}&pn={current_page}'
            soup = self.get_soup(page_link)

            try:
                job_offers = self.get_job_offers_from_page(soup)
                last_job_publish_date = self.extract_job_publish_date(job_offers[-1])
                links = list()
                if last_job_publish_date <= self.end_date:
                    links = self.extract_job_links(job_offers, scrape_dates)
            except AttributeError:
                self.logger.exception(f"Tag was not found in the HTML of the current page. Please check if the HTML of the source webpage was changed: {page_link}")
                raise TagNotFoundException

            for job in links:
                try:
                    data_per_contract = self.extract_job_data(job)
                    all_data += data_per_contract
                except TagNotFoundException:
                    pass

                
            if self.running == True:
                next_button = soup.find('li', class_='pagination_element pagination_element--next')
                if not next_button:     # if more pages
                    self.running = False

            current_page += 1

        if len(all_data) > 1:
            df = self.write_to_df(all_data)
            file_key = self.target_bucket.generate_file_key(self.start_date, self.end_date, self.target_file_format)    
            self.target_bucket.write_df_to_s3(df, file_key)
            MetaProcess.update_meta_file(self.target_bucket, self.logger, self.metafile_key, scrape_dates)
            self.logger.info(f"Scraping finished. File '{file_key}' was created with {len(all_data)-1} records.")
        else:
            self.logger.info('No new records were found. The data file was not created.')


    def get_soup(self, page_link: str) -> BeautifulSoup:
        self.driver.get(page_link)
        html = self.driver.page_source
        soup = BeautifulSoup(html, self.parser)
        return soup


    def get_job_offers_from_page(self, soup: BeautifulSoup) -> ResultSet:
        search_result = soup.find('div', class_='results')
        jobs_offers = search_result.find_all(
                lambda tag: tag.name == 'li' and tag.get('class') == ['results__list-container-item'])
        return jobs_offers


    def extract_job_publish_date(self, job: Tag) -> date:
        publish_date = job.find('span', class_='offer-actions__date').find_all(text=True)
        if len(publish_date) < 5:
            publish_date.insert(0, ' ')

        day_month = publish_date[2].split(' ') 
        day = int(day_month[0])
        month = int(self.month_names[day_month[1]])
        year = int(publish_date[4].replace('\n', ''))

        publish_date = date(year, month, day)
        return publish_date


    def extract_job_links(self, job_offers: list, scrape_dates: list) -> list:
        all_links = list()
        for job in job_offers:
            publish_date = self.extract_job_publish_date(job)

            if publish_date < self.start_date:
                self.running = False
                return all_links
            if publish_date not in scrape_dates:
                continue

            regions = job.find_all('a', class_='offer-regions__label')
            if not regions:     # single location (one link)
                link_tag = job.find('a', class_='offer-details__title-link')
                all_links.append({'link': link_tag['href'], 'publish_date': publish_date, 
                                'regions': []})
            else:               # multiple locations (multiple links)
                all_links.append({'link': regions[0]['href'], 'publish_date': publish_date, 
                                'regions': [region.text for region in regions]})

        return all_links


    def extract_job_data(self, job: dict, encoding: str='utf-8') -> list:
        self.driver.get(f"{job['link']}#company-details")
        self.wait_for_employer_profile(job['link'])
        job_html = self.driver.page_source
        job_html = BeautifulSoup(job_html, 'lxml')


        basic_job_data = self.get_basic_job_data(job_html, job)
        categories_data = self.get_categories_data(job_html)
        employer_data = self.get_employer_data(job_html)
        data_per_contract = self.get_contract_specific_data(job_html)
      
        for contract in data_per_contract:
            contract += basic_job_data + categories_data + employer_data
        
        return data_per_contract


    def wait_for_employer_profile(self, job_link: str, wait_time: int=5) -> None:
        try:
            dummy = WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,".employer-profile-WcUgc")))
        except TimeoutException:
            #self.logger.warning(f"Company profile link tag did not load within {wait_time} seconds. Please check if the HTML of the source webpage was changed: {job_link}")
            raise TagNotFoundException  


    def get_basic_job_data(self, job_html: BeautifulSoup, job: dict) -> list:
        job_link = job['link']
        publish_date = job['publish_date']

        id_start_position = job_link.rfind(',') + 1
        id_end_position = job_link.find('?', id_start_position)
        offer_id = job_link[id_start_position:id_end_position]

        if len(job['regions']) > 0:
            location = '|'.join(job['regions'])
        else:
            location_info = job_html.find('div', class_='offer-viewumlDlF').find('div').find_all()
            location = location_info[0].text

        try:
            position_title = job_html.find('h1', class_='offer-viewkHIhn3').text
        except AttributeError:
            position_title = None

        work_schedule, position_type, work_mode = [None]*3
        benefit_list = job_html.find_all('div', class_='offer-viewXo2dpV')
        for element in benefit_list:
            if element['data-test'] == 'sections-benefit-work-schedule-text':
                work_schedule = element.text
            elif element['data-test'] == 'sections-benefit-employment-type-name-text':
                position_type = element.text
            elif element['data-test'] == 'sections-benefit-work-modes-text':
                work_mode = element.text

        return [offer_id, job_link, publish_date, position_title, location, work_schedule, work_mode, position_type]


    def get_categories_data(self, job_html: BeautifulSoup) -> list:
        categories = None

        category_panel = job_html.find('ul', class_='offer-viewEmkiAc')
        if category_panel is None:
            return categories
     
        category_tiles = category_panel.find_all('li', recursive=False)
        
        for index, category in enumerate(category_tiles):
            if index == 3:    #3 - subcategories
                categories = category.find('a')['href'][-7:]
                other_categories = category.find_all('a', class_='offer-vieweWtPBJ')
                for other_category in other_categories:
                    categories += ", " + other_category['href'][-7:]

        return [categories]


    def get_employer_data(self, job_html: BeautifulSoup, encoding: str='utf-8') -> list:
        employer_name, employer_address, employer_tax_id = [None]*3

        employer_profile_link = job_html.find('a', class_='employer-profileiHwZjJ')
        if employer_profile_link is None:
            employer_profile_link = job_html.find('a', class_='ep-profile-link')
        
        if employer_profile_link is None:
            return [employer_name, employer_address, employer_tax_id]

        employer_profile_link = employer_profile_link['href']
        employer_html = requests.get(employer_profile_link)
        employer_html.encoding = encoding
        employer_html = BeautifulSoup(employer_html.text, 'lxml')

        with self.ignored(AttributeError):
            employer_name = employer_html.find('div', class_='title-container').find('h1').text
        with self.ignored(AttributeError):
            employer_data = employer_html.find('div', class_='box-content').find('div', class_='text')
            with self.ignored(AttributeError):
                employer_address = employer_data.find('p', itemprop='address').text
            with self.ignored(AttributeError):
                employer_tax_id = employer_data.find('p', itemprop='taxID').text
    
        return [employer_name, employer_address, employer_tax_id]


    def get_contract_specific_data(self, job_html: BeautifulSoup) -> list:
        contracts_list = []

        salaries = job_html.find_all('strong', class_='offer-viewLdvtPw')
        salary_units = job_html.find_all('span', class_='offer-viewSGW6Yi')
        contracts = job_html.find_all('div', class_='offer-vieweHKpRl')

        if len(contracts) < 1:
            contracts.append(None)
            benefit_list = job_html.find_all('div', class_='offer-viewXo2dpV')
            for element in benefit_list:
                if element['data-test'] == 'sections-benefit-contracts-text':
                    contracts[0] = element

        for index, salary in enumerate(salaries):
            salary_max, salary_min = [None]*2

            with self.ignored(AttributeError):
                salary_max = salary.find('span', class_='offer-viewYo2KTr').text
            with self.ignored(AttributeError):
                salary_min = salary.find('span', class_='offer-viewZGJhIB').text
            
            contracts_list.append([contracts[index].text, salary_min, salary_max, salary_units[index].text])
        
        if len(contracts_list) < 1:
            contracts_list = [[None, None, None, None]]

        return contracts_list
        

    def write_to_df(self, data: list) -> pd.DataFrame:
        output = StringIO()
        csv_writer = writer(output)
        for row in data:
            csv_writer.writerow(row)

        output.seek(0) # we need to get back to the start of the StringIO
        df = pd.read_csv(output)
        return df


    @contextmanager
    def ignored(self, *exceptions):
        try:
            yield
        except exceptions:
            pass