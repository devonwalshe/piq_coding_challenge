import unittest, types
from jobs.filter_csv.pipeline import Pipeline, conf
from jobs.filter_csv.validate import d_schema, csv_schema
import pkg_resources
from pyspark.sql.dataframe import DataFrame


class PipelineTest(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    super(PipelineTest, self).__init__(*args, **kwargs)
    self.pipeline = Pipeline()
    self.bucket = conf['S3_BUCKET']
    self.test_df = self.test_data()
  
  def test_data(self):
    '''
    Wanted to use the sample csv as a deterministic test case for data but couldn't find a suitable way
    to get the file available on all nodes
    '''
    # contents = pkg_resources.resource_string('jobs.filter_csv', 'sample_data.csv')
    # f = open('/tmp/sample_data.csv', 'w')
    # f.write(contents.decode())
    # f.close()
    key = next(self.pipeline._fetch_csvs(self.bucket))
    df = self.pipeline.spark.read.load('s3a://{}/{}'.format(self.bucket, key), format="csv", sep=",", header="true", schema=csv_schema)
    return(df)
    
## Test conf variables
  def test_conf(self):
    conf_keys = sorted(list(conf.keys()))
    keys = ['AWS_KEY',
      'AWS_KEYFILE',
     'AWS_SECRET',
     'AWS_SUBNET',
     'PG_DB',
     'PG_HOST',
     'PG_PASS',
     'PG_USER',
     'S3_BUCKET']
    self.assertEqual(conf_keys, keys)

## Test fetching data works
  def test_fetch(self):
    gen = self.pipeline._fetch_csvs(self.bucket)
    self.assertIsInstance(gen, types.GeneratorType)
    key = next(gen)
    self.assertIsInstance(key,str)

## Test loading a CSV works
  def test_load(self):
    key = next(self.pipeline._fetch_csvs(self.bucket))
    df = self.pipeline.load_csv(key)
    self.assertIsInstance(df, DataFrame)
    
## Test CSV is in the right format
  def test_val(self):
    df = self.test_df
    df = self.pipeline.validate_df(df)
    self.assertIsInstance(df, DataFrame)
    
## Test processing
  def test_processing(self):
    ### Set up local test_data
    df = self.test_df
    df = self.pipeline.process_df(df)
    ### did desc change to description
    self.assertFalse('desc' in df.columns)
    ### Check one value we knew to have more precision
    self.assertEqual(df.take(1)[0].total_pymnt, 15824.0)

## Test filters work
  def test_filter(self):
    df = self.test_df
    df = self.pipeline.filter_df(df)
    ### count records
    self.assertEqual(df.count(), 10)

  def test_postgres_connection(self):
    pg_url = "jdbc:postgresql://{host}:5432/{db}".format(host=conf['PG_HOST'], db=conf['PG_DB'])
    pg_props = {"user": conf['PG_USER'], "password": conf["PG_PASS"], "driver": "org.postgresql.Driver"}
    response = self.pipeline.spark.read.jdbc(pg_url, table='loans', properties = pg_props)
    self.assertIsInstance(response, DataFrame)
