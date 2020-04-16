from jobs.filter_csv.pipeline import Pipeline
from jobs.filter_csv.test import PipelineTest


def analyze(sc, **kwargs):
  p = Pipeline()
  p.run()

def test():
  '''
  Runs tests
  '''
  pt = PipelineTest()
  pt.test_conf()
  pt.test_fetch()
  pt.test_load()
  pt.test_val()
  pt.test_processing()
  pt.test_filter()
  pt.test_postgres_connection()
  print("#### \t All tests passed \t ####")