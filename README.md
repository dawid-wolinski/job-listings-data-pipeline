About
===

In this pipeline the data from the Polish job listings website https://www.pracuj.pl/ is extracted, transformed and loaded into data warehouse. 
It consists of two Python scripts: the web scraping tool and the transformation tool.

## Architecture
![diagram-pipeline](https://user-images.githubusercontent.com/45266505/165622680-93a170a0-90ba-4d4b-9748-fb5248a10b4f.png)

## Web Scraping Tool
It uses Beautiful Soup as a main framework for scraping the website. Since some content in the source HTML is dynamic and cannot be extracted with Beautiful Soup, the Selenium library is used as a second scraping tool. 
It is configured to look for job listings based on the day they were published by setting the starting date and optionally the end date which by default is set to today's date. Once data from particular day is extracted, it (the date) is written into the metafile to prevent the tool from scraping the same job listings. The job data is written into apache parquet files and stored in the AWS S3 Bucket.

## Data Transforming Tool
Once new file appears in the S3 Bucket, the data is transformed using Pandas library and then split into facts and dimensions. The facts in this case are salaries offered by the employers. Each new dimension and fact is assigned with its unique surrogate key. Finally, using the Amazon Redshift Python connector, the new data is loaded into AWS Redshift which serves as a data warehouse. Names of files from which data was transformed and loaded into DWH are written into the metafile. 

## Orchestration
The pipeline is managed using Argo Workflows which is a Kubernetes orchestration enginge allowing to schedule containerised applications. For this purpose Docker images of both tools (Web Scraping and Data Transforming) were created. The Kubernetes deployment file specifies DAG (Directed Acyclic Graph) with two tasks - each responsible for pulling specific Docker image and running one of the tools within a container.

<img src="https://user-images.githubusercontent.com/45266505/168036773-6b9da96a-8490-493a-8f5d-d31220b54280.png" width=30% height=30%>

The DAG is scheduled to run everyday at 4:00 a.m. UTC. It starts with the Web Scraping task and once it is finished, the Data Transformation task is executed. 
Kubernetes is hosted on t3.micro Amazon EKS Cluster (or was until I found out it's not part of the AWS Free Tier services). 

## Data Warehouse Model
![job-salaries](https://user-images.githubusercontent.com/45266505/165736559-1a3e4948-c8ff-47f2-a8bf-4d9005aca3f5.png)

The DWH model is using a star schema. Measurement gathered in the DWH are salaries offered by employers which is stored in the fact table, while all other data forms dimension tables. Additionally, two bridge tables were created (https://www.kimballgroup.com/2012/02/design-tip-142-building-bridges/) in order to accommodate many-to-many relationships between the fact table and location and category dimension tables. Since salaries visible in the job offers are often given in range, the salary fact table consists of minimum salary and maximum salary (if salary is not given in range then minimum = maximum).
