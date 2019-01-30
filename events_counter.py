from __future__ import print_function
from spark_utils import spark_session
from file_utils import merge_output_files, delete_folder
import os
import time
import sys
import getopt


LOCATION_CSV = 'location.csv'
REGION_CSV = 'region.csv'
RESULT_CSV = 'result.csv'
OUTPUT_DIR = 'output'
CSV_HEADER = '"region_name","date","count"\n'
USAGE = "Usage: spark-submit events_counter.py [-d] <data_directory>\n\t-d\tDeletes auxiliary output directory."


def process(spark, data_directory, delete_auxiliary_folder):
    """
    Reads location and region csv files, groups the data by region and date, and outputs the results to a single
    result.csv file.
    :param spark: Spark session
    :param data_directory: directory with input and output data files
    :param delete_auxiliary_folder: boolean flag whether the partial csv files should be preserved or deleted
    :return: None
    """
    output_directory = os.path.join(data_directory, OUTPUT_DIR)
    output_file = os.path.join(data_directory, RESULT_CSV)
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

    # Merge output files to one
    merge_output_files(output_directory, output_file, CSV_HEADER)

    if delete_auxiliary_folder:
        delete_folder(output_directory)


def main(directory, delete_auxiliary_folder):
    # Create a new Spark session
    with spark_session("QriousEventCounter") as spark:
        t0 = time.time()
        # run Spark application - count events per region and date
        process(spark, directory, delete_auxiliary_folder)
        t1 = time.time() - t0
        print('Processing took {} seconds.'.format(t1))


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
        raise ValueError("Error: Unknown option.\n" + USAGE)

    if len(args) != 1:
        raise ValueError("Error: Missing <data_directory>.\n" + USAGE)

    directory = args[0]
    return directory, delete_auxiliary_files


if __name__ == '__main__':
    try:
        folder, delete_output_folder = parse_argv(sys.argv)
    except ValueError as e:
        print(e)
        sys.exit(-2)
    main(folder, delete_output_folder)
