## Job Listings to Data Warehouse Pipeline

This pipeline extracts data from Polish job listings website https://www.pracuj.pl/, transforms and loads it into the data warehouse. 
It consists of two scripts: the web scraping tool and transformation tool.

### Architecture
![diagram-pipeline](https://user-images.githubusercontent.com/45266505/165622680-93a170a0-90ba-4d4b-9748-fb5248a10b4f.png)

### Web Scraping Tool
Written in Python, it uses Beautiful Soup as a main framework for scraping the website. Since some content in the source HTML is dynamic and cannot be extracted with Beautiful Soup, the Selenium library is used as a second scraping tool. 
It is configured to look for job listings based on the day they were published by setting the starting date and optionally the end date which by default is set to today's date. Once data from particular day is extracted, it (the date) is written into the metafile to prevent the tool from scraping the same job listings. The job data is written into apache parquet files and stored in the AWS S3 Bucket.

### Data Transforming Tool
Once new file appears in the S3 Bucket, the data is transformed using Pandas library and then split into facts and dimensions. The facts in this case are salaries offered by the employers. Each new dimension and fact is assigned with its unique surrogate key. Finally the new data is loaded into AWS Redshift which serves as a Data Warehouse. Names of files from which data was transformed and loaded to DWH are written into the metafile. 

### Orchestration
The pipeline is managed using Argo Workflows which is a Kubernetes orchestration enginge allowing to schedule containerised applications. Both, the Web Scraping and Data Transforming are run in the form of separate Docker containers which are built by Kubernetes based on their Docker images. The workflow is scheduled to run everyday at 4:00 a.m. UTC. It starts with the Web Scraping job and once it is finished, the Data Transformation is executed. 
Kubernetes is hosted on t3.micro EKS Cluster (or was until I found out it's not part of the AWS Free Tier services). 

### Data Warehouse Model
![job-salaries](https://user-images.githubusercontent.com/45266505/165736559-1a3e4948-c8ff-47f2-a8bf-4d9005aca3f5.png)

The DWH model is using a star schema. Measurement gathered in the DWH are salaries offered by employers which is stored in the fact table, while all other data forms dimension tables. Additionally, two bridge table were created (https://www.kimballgroup.com/2012/02/design-tip-142-building-bridges/) in order to accommodate many-to-many relationships between the fact table and location and category dimension tables. Since salaries visible in the job offers are often given in range, the salary fact table consists of minimum salary and maximum salary (if salary is not given in range then minimum = maximum).
