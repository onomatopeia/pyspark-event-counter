# Prerequisites



# Execution

The code was written in Python 3 and tested under Python 3.6.5, but is (to the best of my knowledge) compliant with 
earlier versions of Python 3.x as well as with Python 2.7. 
  
Below are instruction how to execute this Spark application on [local environment](#local-environment) and on [AWS 
EMR](#aws-emr). 
In either case, **clone this repository to your local machine** first. 

## Local environment

### Prerequisites

To run this code locally you need to have Spark installed and `pyspark` Python module.

### Test data
In `test/data` folder you can find a `region.csv` file with a list of districts in New Zealand. 

Run `python3 data_generator.py` (or `python data_generator.py` for Python 2.7) to generate a `location.csv` file. By 
default it will be 1GB; to set a different size see help: `python3 data_generator.py -h`

### Running Spark application

Usage: `spark-submit events_counter.py [-d] <data_folder>`

`-d` Spark creates multiple csv output files with partial results. These files are merged into a single `result.csv` 
file. Use this flag to delete partial output files. By default all partial files are preserved and can be used for 
debugging or verification purposes.    

Run `spark-submit events_counter.py -h` to see help. 

## AWS EMR via AWS CLI

### Prerequisites
- an S3 bucket with data folder containing `region.csv` and `location.csv` files
- an S3 bucket where the Spark application can be copied to
- configured AWS CLI 

### Running Spark application

1. Create a variable with the name of the bucket where you will upload the Python script, like this:
```set bucket=s3://emr-qrious-bucket/eventsQuery```
```set data_bucket=s3://emr-qrious-bucket/wordcount2/data```
```aws s3 sync . s3://emr-qrious-bucket/eventsQuery --exclude "*" --include "*.py"```

2. Copy the Python script `events_counter.py` to `bucket`:
```aws s3 cp events_counter.py %bucket%/events_counter.py```

3. [Optional] Create a key-pair for EC2 (and save the results to a json file)
```aws ec2 create-key-pair --key-name QriousKeyPair > QriousKeyPair.json```

4. Create a EMR cluster. Save the ClusterId displayed in the output.
```aws emr create-cluster --name "Qrious Application" --release-label emr-5.20.0 --applications Name=Spark --ec2-attributes KeyName=QriousKeyPair --instance-type m4.large --instance-count 3 --use-default-roles --query "ClusterId" --output text```
The Cluster Id will be printed out (like `j-2EQP9PD5PRDYL`), save it for the next step.

5. Add steps
```--steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://emr-qrious-bucket/eventsQuery/events_counter.py","s3://emr-qrious-bucket/wordcount2/data"],"Type":"CUSTOM_JAR","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar","Properties":"","Name":"Event Counter"}]'```

### AWS EMR via Boto3

1. Copy `aws.cfg.template` file as `aws.cfg` and fill with data. 
2. Run `boto_executor.py`
# Unit tests

In terminal go to the main directory of this project and execute `pytest`.