import boto3, yaml, argparse

conf = yaml.safe_load(open('./src/jobs/filter_csv/conf.yaml', 'r'))
connection = boto3.client('emr',region_name='us-east-1',aws_access_key_id=conf['AWS_KEY'], aws_secret_access_key=conf['AWS_SECRET'],)

def launch_cluster(conf=conf, cluster_name="piq_test_cluster"):
  cluster_id = connection.run_job_flow(Name='piq_test',ReleaseLabel='emr-5.29.0',
      Applications=[
          {
              'Name': 'Spark'
          },
      ],
      Instances={
          'InstanceGroups': [
              {
                  'Name': "Master",
                  'Market': 'ON_DEMAND',
                  'InstanceRole': 'MASTER',
                  'InstanceType': 'm4.large',
                  'InstanceCount': 1,
              },
              {
                  'Name': "Slave",
                  'Market': 'ON_DEMAND',
                  'InstanceRole': 'CORE',
                  'InstanceType': 'm4.large',
                  'InstanceCount': 1,
              }
          ],
          'Ec2KeyName': '{}'.format(conf['AWS_KEYFILE']),
          'KeepJobFlowAliveWhenNoSteps': True,
          'TerminationProtected': False,
          'Ec2SubnetId': '{}'.format(conf['AWS_SUBNET'])
      },
      Steps=[
          {
              'Name': 'file-copy-step',   
                      'ActionOnFailure': 'CONTINUE',
                      'HadoopJarStep': {
                          'Jar': 's3://Snapshot-jar-with-dependencies.jar',
                          'Args': ['test.xml', 'emr-test', 'kula-emr-test-2']
                      }
          }
      ],
      VisibleToAllUsers=True,
      JobFlowRole='EMR_EC2_DefaultRole',
      ServiceRole='EMR_DefaultRole',
  )
  print(cluster_id)
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Launch an EMR Cluster')
  parser.add_argument('name')
  args=parser.parse_args()
  launch_cluster(cluster_name=args.name)