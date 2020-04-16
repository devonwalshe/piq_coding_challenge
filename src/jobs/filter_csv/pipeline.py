###
import sys, os, yaml, pkg_resources, boto3, time
from functools import reduce
from botocore.client import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from jobs.filter_csv.validate import d_schema, csv_schema

### set configuration from local file
conf_string = pkg_resources.resource_string('jobs.filter_csv', 'conf.yaml')
conf = yaml.safe_load(conf_string.decode())

class Pipeline(object):
  
  def __init__(self):
    self.spark = self._gen_spark()
    self.sc = self.spark.sparkContext
    self.current_key = None
    self.s3 = boto3.client('s3', aws_access_key_id=conf['AWS_KEY'], aws_secret_access_key=conf['AWS_SECRET'])
    self.bucket = conf['S3_BUCKET']
    self.local_storage = False
    self.csv_schema = csv_schema
  
  def run(self):
    '''
    Executes the pipeline of functions, piping the output of one function to the next
    '''
    bucket = self.bucket
    pipeline = (self.load_csv, self.validate_df, self.process_df, self.filter_df, self.save_df, self.post_process)
    for s3_key in self._fetch_csvs(bucket):
      self.current_key=s3_key
      ### Run pipeline
      start = time.time()
      processed_csv = reduce(lambda result, function: function(result), pipeline, s3_key)
      end = time.time()
      ### Simple feedback
      print("Processed {} file in {} seconds".format(s3_key, end-start))
      
        
  def load_csv(self, s3_key):
    '''
    Reads the CSV file from s3 and returns a Spark dataframe. Defaults to use the s3a 
    protocol. If its not available you can set 'local_storage' on the instance and the 
    file will be downloaded and read from the local filesystem.
    '''
    ### Download the file
    ### if AWS jars are not available:
    if self.local_storage:
      path = '/tmp/{}'.format(s3_key)
      try:
        self.s3.download_file(self.bucket, s3_key, '/tmp/{}'.format(s3_key))
      except:
        raise Exception("Error: ", sys.exc_info[0])
    else:
      path = "s3a://{}/{}".format(self.bucket, s3_key)
    try:
      df = self.spark.read.load(path, format="csv", sep=",", header="true", schema=self.csv_schema)
      return(df)
    except:
      raise Exception("Error: ", sys.exc_info()[0])

  
  def validate_df(self, df):
    '''
    Checks column types of the loaded DF against a schema, confirms name,
    type and count of columns. 
    '''
    if not sorted(df.dtypes) == d_schema:
      raise TypeError("Error: Loaded CSV schema does not match validation schema")
    return(df)
  
  def process_df(self, df):
    '''
    Processes the CSV with some initial transformations
    '''
    ### round all double columns
    doubles = [x[0] for x in list(filter(lambda x: x[1]=='double', df.dtypes))]
    for col in doubles:
      try:
        df = df.withColumn(col, f.round(df[col], 2))
      except:
        raise Exception("Error: ", sys.exc_info()[0])
    ### Rename desc column to description for PSQL
    df = df.withColumnRenamed('desc', 'description')
    ### Return
    return(df)

  
  def filter_df(self, df):
    '''
    Filter the dataframe based on the supplied instructions
    '''
    try:
      df = df.filter(df.loan_status != "Charged Off")\
             .filter(df.purpose != "other")\
             .filter(df.last_fico_range_low >= 700)
    except:
      raise Exception("Error: ", sys.exc_info()[0])
    return(df)
  
  def save_df(self, df):
    '''
    Saves the filtered dataframe to postgres
    '''
    pg_url = "jdbc:postgresql://{host}:5432/{db}".format(host=conf['PG_HOST'], db=conf['PG_DB'])
    pg_props = {"user": conf['PG_USER'], "password": conf["PG_PASS"], "driver": "org.postgresql.Driver"}
    try:
      df.write.jdbc(url=pg_url, table="loans", mode='append', properties=pg_props)
    except:
      raise Exception("Error: ", sys.exc_info())
  
  def post_process(self, df):
    '''
    Post processing steps: remove the file from s3, set current_key to None
    '''
    try: 
      self.s3.delete_object(Bucket=self.bucket, Key=self.current_key)
    except:
      raise Exception("Error:", sys.exc_info())
    self.current_key = None
    return(df)
  
  ### private
  
  def _gen_spark(self):
    '''
    Initialises and configures spark
    '''
    spark = SparkSession.builder.appName("PeerIQ Coding Challenge").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", conf['AWS_KEY'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", conf['AWS_SECRET'])
    return(spark)

  def _fetch_csvs(self, bucket):
    '''
    Generator function to read a potentially infinite number of paged objects from an s3 bucket
    '''
    #validate bucket 
    try:
      boto3.resource('s3').meta.client.head_bucket(Bucket=bucket)
    except ClientError:
      raise("Bucket does not exist or you do not have access")
    s3 = boto3.client('s3')
    kwargs={"Bucket":bucket}
    paginator = s3.get_paginator("list_objects_v2")
    ### Paginate through s3 resources
    for page in paginator.paginate(**kwargs):
      try:
        contents = page['Contents']
      except KeyError:
        raise KeyError('No pages found', sys.exc_info()[0])
        break
      for obj in contents:
        key = obj['Key']
        yield key
