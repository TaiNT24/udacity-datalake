# README

## Description
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project use Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. 

This project load data from S3, process the data into analytics tables using Spark, and load them back into S3. This Spark process on a cluster using AWS.


## Source files
    .
    ├── dl.cfg                   # Config file
    ├── etl.py                   # ETL python file to process data
    └── README.md


## Instructions

1. You have to provide your AWS access & secret key in file: `dl.cfg`. Ensure your access key has permission to write object to S3. 

    Reference: [Link](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)


    #### `AWS_ACCESS_KEY_ID`: your access key
    #### `AWS_SECRET_ACCESS_KEY`: your secret access key
    #### `S3_BUCKET_OUTPUT`: your S3 bucket that data will output

2. Run script:

    #### `python etl.py`