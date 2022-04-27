### Job Listings to Data Warehouse Pipeline

This pipeline extracts data from Polish job listings website https://www.pracuj.pl/, transforms and loads it into the data warehouse. 
It consists of two scripts: the web scraping tool and transformation tool.

## Web Scraping Tool
Written in Python, it uses Beautiful Soup as a main framework for scraping the website. As some content in the source HTML is dynamic and cannot be extracted with Beautiful Soup, the Selenium library is used as a second scraping tool. 
It is configured to look for job listings based on the day they were published by setting the starting date and optionally the end date which by default is set to today's date. Once data from particular day is extracted, it (the date) is written into metafile to prevent the tool from scraping the same job listings. The data job data is written into apache parquet files and stored in the AWS S3 Bucket.

## Data Transforming Tool
Once new file appears in the S3 Bucket, the data is transformed using Pandas library and then split into facts and dimensions. The facts in this case are salaries offered by the employers. Finally the new data is loaded into AWS Redshift which serves as a Data Warehouse. Names of files from which data was transformed and loaded to DWH are written into metafile. 


![diagram-pipeline](https://user-images.githubusercontent.com/45266505/165622680-93a170a0-90ba-4d4b-9748-fb5248a10b4f.png)
