# dataengineering-youtube-analysis-etl-project--234
# Data Engineering YouTube Analysis ETL Project

## Overview
This project is an end-to-end ETL pipeline for analyzing YouTube video trends. The workflow demonstrates how to collect, process, and visualize data from Kaggle using AWS services, including S3, Lambda, Glue, and Athena. It is designed for hands-on learning of cloud-based data engineering techniques.
![Architecture Overview](architecture.jpeg)

## Project Goals
- Data ingestion from multiple sources, including Kaggle datasets.
- ETL pipeline to clean, transform, and prepare data.
- Centralized data lake storage using Amazon S3.
- Scalable and serverless processing with AWS Lambda and Apache Spark.
- Data cataloging and querying using AWS Glue and Athena.
- Visualization of insights through querying and dashboards.
- Secure cloud infrastructure managed via AWS IAM.

## Services Used
- **Amazon S3:** Object storage for raw and processed datasets with high scalability and availability.
- **AWS Lambda:** Serverless compute to automate ETL workflows for data transformation.
- **AWS Glue:** Managed data cataloging and ETL orchestration service.
- **AWS Athena:** Interactive querying service allowing SQL queries directly on data stored in S3.
- **Apache Spark:** Distributed data processing framework for complex ETL transformations.
- **AWS IAM:** Identity and Access Management for secure permissions and resource access.

## Repository Structure

- `architecture.jpeg` – Visual diagram of the project architecture and data flow.
- `lambda_function.py` – AWS Lambda script for processing YouTube data and transforming JSON into flattened schemas using Pandas and AWS Wrangler.
- `s3_cli_commands.sh` – Shell commands for interacting with AWS S3 such as uploading/downloading files, listing buckets, etc.
- `spark_script.py` – ETL script for transforming and analyzing data using Apache Spark with PySpark.

## Dataset
The project uses the Kaggle YouTube trending video dataset containing daily statistics on trending videos across various regions. It includes metadata such as video title, channel, publication time, view counts, likes, dislikes, tags, and category IDs.  
[Kaggle Dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new)

## Workflow Steps

1. **Data Collection:** Collect trending YouTube video data from Kaggle and upload to S3.
2. **ETL Processing:** 
   - Use `lambda_function.py` for automated, serverless data cleansing and transformation.
   ![Lambda Function Setup](screenshots/lambda_function.png)
   - Use `spark_script.py` for scalable ETL operations.
   ![Spark Script Execution](screenshots/spark_execution.png)
   - Shell commands in `s3_cli_commands.sh` help manage data storage.
   ![S3 Data Upload](screenshots/s3_upload.png)
   
4. **Analysis:** Query processed data with Athena and further explore with dashboards or notebooks.
   ![Athena Query Results](screenshots/athena_results.png)
6. **Visualization:** Architecture diagram illustrates cloud integration and workflow.

## Technologies

- AWS S3, Lambda, Glue, Athena
- Python (Pandas, AWS Wrangler, PySpark)
- Shell scripting for cloud operations

## How to Use

1. Review the architecture diagram for an overview.
2. Execute `s3_cli_commands.sh` to manage dataset storage.
3. Deploy Lambda function for automated ETL processing.
4. Run analysis with Spark for complex transformations.
5. Query results with Athena for insights.




Feel free to fork, modify, and contribute! For detailed steps, see code comments and project files.

