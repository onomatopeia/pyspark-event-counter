from __future__ import print_function
import os
import sys
import getopt
from pyspark.sql import SparkSession
from contextlib import contextmanager


@contextmanager
def spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


LOCATION_CSV = 'location.csv'
REGION_CSV = 'region.csv'
RESULT_CSV = 'result.csv'
OUTPUT_DIR = 'output'
CSV_HEADER = '"region_name","date","count"\n'
USAGE = "Usage: spark-submit events_counter.py [-d] <data_directory>\n\t-d\tDeletes auxiliary output directory."


def process(spark, data_directory, output_directory):
    """
    Reads location and region csv files, groups the data by region and date, and outputs the results to a single
    result.csv file.
    :param spark: Spark session
    :param data_directory: directory with input and output data files
    :param delete_auxiliary_folder: boolean flag whether the partial csv files should be preserved or deleted
    :return: None
    """

    # data frames
    df = spark.read.csv(os.path.join(data_directory, LOCATION_CSV), header=True)
    regions = spark.read.csv(os.path.join(data_directory, REGION_CSV), header=True)

    """
    Process the data frame with location data:
    1. group by "region_id" and "date"
    2. count rows in each group
    3. join the data frame with regions data frame on region_id
    4. from the full (joined) data frame select "region_name", "date", and "count" columns
    5. write the output to csv files - this results in multiple csv files
    """
    df.groupBy(['region_id', 'date'])\
        .count()\
        .join(regions, df.region_id == regions.region_id)\
        .select('region_name', 'date', 'count')\
        .write.csv(path=output_directory, quoteAll=True, mode='overwrite', header=True)


def main(directory, delete_auxiliary_folder):
    output_directory = os.path.join(directory, OUTPUT_DIR)
    output_file = os.path.join(directory, RESULT_CSV)

    # Create a new Spark session
    with spark_session("EventCounter") as spark:
        # run Spark application - count events per region and date
        process(spark, directory, output_directory)

    # Merge output files to one - this will work only if the files are local
    if os.path.isdir(output_directory):
        merge_output_files(output_directory, output_file, CSV_HEADER)

        if delete_auxiliary_folder:
            delete_folder(output_directory)


def parse_argv(args):

    if len(args) == 1:
        raise ValueError("Error: Not enough arguments.\n" + USAGE)

    delete_auxiliary_files = False
    try:
        optlist, args = getopt.getopt(args[1:], 'dh')
        for opt, arg in optlist:
            if opt == '-h':
                print(USAGE)
                sys.exit()
            if opt in ("-d", "--delete-aux"):
                delete_auxiliary_files = True
    except getopt.GetoptError:
        ValueError("Error: Unknown option.\n" + USAGE)

    if len(args) != 1:
        raise ValueError("Error: Missing <data_directory>.\n" + USAGE)

    directory = args[0]
    return directory, delete_auxiliary_files


def rewrite_csv(input_file, output_file):
    with open(input_file, 'r', newline='') as csvin:
        with open(output_file, 'a', newline='') as csvout:
            for idx, line in enumerate(csvin):
                if idx > 0:  # ignore header
                    csvout.write(line)


def merge_output_files(output_directory, output_file, csv_header):
    with open(output_file, 'w+') as csvout:
        csvout.write(csv_header)
    for file in os.listdir(output_directory):
        if file.endswith('.csv'):
            rewrite_csv(os.path.join(output_directory, file), output_file)


def delete_folder(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
    try:
        os.rmdir(folder)
    except Exception as e:
        print("Could not remove folder. \n{}".format(e))


if __name__ == '__main__':
    try:
        folder, delete_output_folder = parse_argv(sys.argv)
    except ValueError as e:
        print(e)
        sys.exit(-2)
    main(folder, delete_output_folder)
