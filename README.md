# Data Engineer Take-Home Challenge: Mini ETL Pipeline.

### Objective

Develop a data processing pipeline that tranforms a large volume of text data from a Kinesis stream into a format suitable for the continuous training a text summarization model. The pipeline should leverage Python and be designed with enterprise-scale data processing tools in mind.

---

### Background

You're part of a dynamic team at a company specializing in NLP services dedicated to assisting news aggregators and content curators. The company's mission is to automate the summarization of articles and reports from diverse sources, enabling rapid access to distilled insights. This task is crucial for staying ahead in the fast-paced world of news and content curation, where the ability to quickly summarize and understand large volumes of text data can provide a significant competitive edge. 


### Overview

The NLP model must be trained on a regular schedule to keep up-to-date on the stylistic elements of modern journalism. Therefore in this challenge, you are entrusted with the development of a data pipeline capable of processing incoming text data efficiently. The ultimate goal is to enhance this data in ways that bolster machine learning (ML) training scenarios. This includes considering how the result is stored, ensuring that it supports efficient data access patterns suitable for large-scale processing and ML model training.

To kickstart your task, you are provided with a sample dataset alongside a foundational setup designed to simulate the ingestion of streaming text data. Your challenge is to extend and optimize this setup in a way explained below.

---

### Objective

Develop a data processing pipeline that retrieves each record from a Kinesis stream, enriches it by adding a calculated feature (e.g., word count) to every record, and computes the average of this metric for all records. The enriched records, now including the additional feature, should then be stored in an S3 bucket, ready for subsequent processing steps. Note that only the enriched records need to be stored; the calculated average does not require storage."


## **Getting Started**

### **Setup Instructions**

1. **Environment Setup**: Ensure Docker and Docker Compose are installed on your system. The provided **`docker-compose.yml`** includes services for LocalStack (simulating AWS S3 and Kinesis).
2. **Running the Pipeline**: Execute ```docker-compose up``` to launch the services. This command initiates a simulated AWS environment for your pipeline's operation. The setup involves starting two containers: one for simulating AWS services like S3 and Kinesis, and another for populating the Kinesis stream (named "MyStream") with approximately 1000 records and creating an S3 bucket ("my-bucket") for storage. The population script runs iteratively, simulating the continuous generation of data. 

### **Note to Candidates**
We understand that real-world data engineering projects often require dealing with incomplete information or making educated guesses about the best approach to a problem. In this challenge, should you encounter any ambiguities or feel that certain details are not specified, you are encouraged to make reasonable assumptions to fill in the gaps. Please document any such assumptions and the rationale behind them in your submission. This will help us understand your thought process and decision-making skills in navigating real-world scenarios.

## **Requirements**

- While not mandatory, you are encouraged to use an enterprise-scale data processing solution such as Metaflow, MLFlow, Spark, Apache Flink, or any other tool suitable for distributed processing of large datasets. Be prepared to justify your choice.
- Incorporate any new dependencies into the docker-compose environment, ensuring they can run locally and offline within Docker containers.
- Design your solution to facilitate machine learning training, focusing on data batching, storage, and retrieval for optimized access in large-scale ML model training.
- Provide documentation on how to set up and run your solution. Outline your design decisions, especially those related to the choice of data processing tools and storage for ML training.

## Submission Instructions

Clone this repository to download the boiler plate. Build your application based on this foundation and update the README accordingly. Submit your completed challenge via email.

## Implemented Solution

### Architecture

The solution implements a batch-oriented ETL pipeline using Python, Boto3, Docker, and LocalStack.

**Data flow:**

Article Publisher → Kinesis (`MyStream`) → Python ETL Pipeline → S3 (`my-bucket`) → Downstream ML Training

### Data Processing

The pipeline:

1. Discovers Kinesis shards and reads available records in batches.
2. Parses each JSON article and calculates its word count using whitespace tokenization.
3. Computes the average word count across all successfully processed records.
4. Writes enriched articles to S3 in batches of up to 500 records.
5. Stores output in JSON Lines (JSONL) format under `training-data/`.

The calculated average is printed to the terminal and is not stored.

### Setup and Execution

**Prerequisites:** Docker Desktop and Docker Compose.

Start LocalStack:

```bash
docker compose up -d localstack
```

Populate the Kinesis stream and create the S3 bucket:

```bash
docker compose run --rm article-publisher
```

Run the ETL pipeline:

```bash
docker compose run --rm etl-pipeline
```

Verify the generated files:

```bash
docker compose exec localstack \
  awslocal s3 ls s3://my-bucket/training-data/
```

### Storage Design for Machine Learning

Each pipeline execution generates a unique run identifier and stores enriched articles under `training-data/run-<RUN_ID>/batch-XXXXX.jsonl`. This prevents output files from being overwritten across executions and keeps each processing run logically separated. Reprocessing the same Kinesis records can still create duplicate data across different run folders; production deduplication would require persistent record tracking or checkpoints.

The enriched articles are stored as JSONL files, allowing downstream training jobs to read individual records sequentially.

Batching reduces the number of S3 write operations compared with uploading every article individually. Each output file contains up to 500 enriched records.

For larger datasets, the storage design could be extended with compression, date-based partitions, and columnar formats such as Parquet.

### Design Decisions

**Python and Boto3:** Selected to keep the implementation lightweight, understandable, and compatible with the provided Docker environment.

**Kinesis:** Simulates ingestion of streaming article data.

**S3:** Provides object storage suitable for downstream data processing and ML training.

**Batch processing:** Limits in-memory accumulation of enriched articles and reduces S3 request overhead.

**JSONL:** Preserves article text and metadata in a format that is straightforward to consume in NLP training workflows.

### Assumptions and Limitations

- The pipeline processes a finite snapshot of the stream rather than running indefinitely.
- Three consecutive empty reads are used as the stopping condition for each shard.
- The publisher is expected to finish before the pipeline runs.
- Records are assumed to contain valid JSON.
- Word count uses whitespace-separated tokens rather than an NLP tokenizer.
- The implementation does not yet provide durable checkpoints, deduplication, or exactly-once processing.
- Repeated executions create separate run folders but may duplicate previously processed records.
- For production-scale streaming, Apache Flink or Spark Structured Streaming could provide distributed processing, state management, and checkpointing.

### Local Test Results

A successful local execution processed **9,381 articles**, calculated an average word count of **138.04**, and stored **19 JSONL batches** in `s3://my-bucket/training-data/`.

An enriched article was retrieved from S3 and verified to contain the additional `word_count` field.