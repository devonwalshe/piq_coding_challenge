
# Readme

## Assumptions
  - S3 bucket only has csv files in it with the same column names and roughly the same casting (errors are caught if not)
  - Client deploy-mode on spark
  - Data input to postgres is simply insert with no primary key constraint, as opposed to upsert/overwrite 
  - AWS Roles 'EMR_EC2_DefaultRole' and 'EMR_DefaultRole' already present on account and the user alreaedy an EC2 keypair available
  
## Environment
- Python 3.6.8
- Spark 2.4.1

## Dependencies:
- python: boto3
- spark: aws package `org.apache.hadoop:hadoop-aws:2.7.1`
- spark: postgres package `org.postgresql:postgresql:9.4.1212`
    
## Running the job
### Task 1 - Spark Application:

  1. Fill in variables in `./src/jobs/filter_csv/conf.yaml.sample` and rename to `conf.yaml` in the same location
  2. Copy `./dist/*` files to master Spark/EMR node
  3. From `./dist/` execute: 
    
    $ spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.1,org.postgresql:postgresql:9.4.1212\
                  --py-files jobs.zip,libs.zip main.py
                  --job filter_csv

  #### How it works
  
  This implementation uses my custom spark job deployment pipeline that provides a single entrypoint `main.py` for multiple spark applications, and dependencies packaged in two zip files, `jobs.zip` and `libs.zip`. 
  
  Definitely overkill for this simple task but wanted to demonstrate where I'm at with writing production spark jobs. 
  
  - Code changes are repackaged with the supplied makefile, using the command `make clean build` and sent to the `dist/` directory 
 - `main.py` accepts arguments `--job`, `--test` and `--job-args` if you want to send any arbitrary kwargs
 -  `--job` specifies the name of job to run, which is searched for in /src/jobs/, and executes the function `analyze` inside that jobs `__init__.py`
 - `--test` runs a `test` function inside the jobs `__init__.py` instead of `analyze` to return your tests
 - `--job-args` specify arbitrary runtime arguments to send to the `analyze` function, ie environment variables or other application specific config
  
  - Implemented a `Pipeline` class that handles the ETL tasks. Starting off using with a paged generator for reading a potentially infinite number of csv files from the bucket. `Pipeline` implements a `run()` method which pipes individual object keys from the generator and output (initially a csv file then a spark dataframe) through a chain of methods:
     - `load_csv()` > `validate_df()` > `process_df()` > `filter_df()` > `postprocess()`
  
  - Postprocessing removes the csv from the bucket so running the pipeline is _roughly_ idempotent
    
  ### Task 2 - launch EMR cluster:
  Make sure aws configuration availables are set in the conf file.
  From root directory: 
	  
	  $ python emr.py your_cluster_name
    
## Notes
- Moved setting the bucket name (and other variables) into a conf.yaml instead of an environment variable specified in the description
 - Left a local file download option in the job, for environments where the aws spark package isn't available
 - Was prescriptive with the schema - cast all financial columns as doubles and left the rest of the cols as they were found.
 - The `readme` didn't specify which amount to round (even though it was clear the `total_pymnt` col was the only one with higher precision) so just rounded all doubles. 
 - No single credit score column so used `last_fico_range_low` as the lower filtering boundary
  
## Speculative additional work
  - Further processing of the dataset (casting columns with % signs as doubles, casting dates as dates etc)
  - No performance tuning on the spark cluster, parallelization of the files etc
  - Improve validation of CSV
  - Add sending messages to an external monitoring service for better runtime debugging
  - Beef up error handling with sending messages and auto healing measures
  - Write more detailed tests and add tests for write and / idempotency
  - Use a custom AMI with required dependencies when launching the EMR cluster
