import boto3
import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('aws.cfg')
    config = config['AWS']
    S3_BUCKET = config['S3_BUCKET']
    S3_FOLDER = config['S3_FOLDER']
    EC2_KEYPAIR = config['EC2_KEYPAIR']
    S3_DATA_URI = config['S3_DATA_URI']

    SCRIPT = 'events_counter.py'
    if S3_FOLDER:
        S3_KEY = '{folder}/{script}'.format(folder=S3_FOLDER, script=SCRIPT)
    else:
        S3_KEY = SCRIPT
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

    # upload file to the S3 bucket
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(SCRIPT, S3_BUCKET, S3_KEY)

    # start an AWS EMR cluster
    REGION = config['REGION']
    client = boto3.client('emr', region_name=REGION)

    response = client.run_job_flow(
        Name="Events Counter Cluster",
        ReleaseLabel='emr-5.20.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 3,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2KeyName': EC2_KEYPAIR
        },
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Hadoop'
            }
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Setup - Copy Files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'Run Spark Application - Events Counter',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/{script}'.format(script=SCRIPT), S3_DATA_URI]
                }
            },
            {
                "Name": "S3DistCp step",
                'HadoopJarStep': {
                    "Jar": "command-runner.jar",
                    "Args": ["s3-dist-cp","--s3Endpoint=s3.amazonaws.com", "--src={uri}/output/".format(uri=S3_DATA_URI),
                             "--dest=hdfs:///output", "--srcPattern=.*csv"]
                },
                "ActionOnFailure": "CANCEL_AND_WAIT"
            },
            {
                "Name": "Hadoop Merge step",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Args": ["hadoop", "fs", "-getmerge",
                             "hdfs:///output", "/home/hadoop/result.csv"],
                    "Jar": "command-runner.jar"
                }
            },
            {
                "Name": "Copy the result to S3",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Args": ["aws", "s3", "cp", "/home/hadoop/result.csv", "{uri}/result.csv".format(
                        uri=S3_DATA_URI)],
                    "Jar": "command-runner.jar"
                }
            },
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    job_flow_id = response['JobFlowId']
    print('Job flow successful submitted. JobFlowId={job_flow_id}'.format(job_flow_id=job_flow_id))
