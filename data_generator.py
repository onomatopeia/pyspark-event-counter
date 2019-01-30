from __future__ import print_function
import os
import random
import time
from datetime import timedelta, date
from math import ceil
import getopt
import sys


class FakeNZEventGenerator:
    """ From https://latitudelongitude.org/nz/: Latitude from -46.56069 to -35.22676 and longitude from -176.55973 to
     178.00417.
    """

    def __init__(self, start_date, date_range_days, region_max, region_min=1):
        self.start_date = start_date
        self.date_range_days = date_range_days
        self.region_max = region_max
        self.region_min = region_min

    @staticmethod
    def latitude():
        # uniform is exclusive on right hand side, but since it is fake data and new zealand is treaded as a rectangle
        # it doesn't matter too much
        return random.uniform(-35.22676, -46.56069)

    @staticmethod
    def longitude():
        x = random.uniform(3.44027, 1.99583)
        if x >= 0:
            return 180 - x
        return -(180 + x)

    def region(self):
        return random.randint(self.region_min, self.region_max)

    def date(self):
        return self.start_date + timedelta(days=random.randint(0, self.date_range_days))

    def event(self):
        return "\"{0:.5f}\",\"{1:.5f}\",\"{2}\",\"{3}\"\n".format(self.latitude(),
                                                                  self.longitude(),
                                                                  self.region(),
                                                                  self.date())

    def events(self, n):
        for _ in range(n):
            yield self.event()


avg_chars_per_line = 42
FILE_1GB = 1073741824


def generate_events(path_to_file, file_size=FILE_1GB, regions=53, start_date=date.today(), date_range=7,
                    chunk_size=23):
    """
    Generates a file of approx file_size size in steps of chunk_size-events units
    :param path_to_file: path to the output file
    :param file_size: desired size of a file
    :param regions: number of regions (assuming consecutive numbering from 1)
    :param start_date: start date for events, inclusive
    :param date_range: number of consecutive days when the events are happening, start_date is day 0
    :param chunk_size: how many events should be written in one go
    :return: execution time, number of generated events
    """
    fake = FakeNZEventGenerator(start_date, date_range, regions)
    t0 = time.time()

    N = int(file_size // avg_chars_per_line)
    n = int(ceil(float(N) / float(chunk_size)))
    with open(path_to_file, "w+") as f:
        f.write("\"lat\",\"long\",\"region_id\",\"date\"\n")
        for _ in range(n):
            f.write("".join(fake.events(chunk_size)))

    t1 = time.time()
    return t1 - t0, n*chunk_size


def main(file_size, output_folder):
    output_file = os.path.join(output_folder, 'location.csv')
    t, n = generate_events(output_file, file_size)
    output_file_size = os.path.getsize(output_file)
    print("{} fake events generated in {} seconds and written to {} [{}]".format(n, t, output_file, output_file_size))


def read_args(argv):
    usage_info = 'Usage:\n\tpython3 data_generator.py [-s <file_size>] <output_folder>\nor\n\tpython3 ' \
                 'data_generator.py [--size <file_size>] <output_folder>'
    file_size = FILE_1GB
    if len(argv) == 1:
        print("Error: Not enough arguments.\n"+usage_info)
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(argv[1:], "hs:", ["size="])
    except getopt.GetoptError:
        print("Error: Unknown options\n"+usage_info)
        sys.exit(-2)
    if len(args) != 1:
        print("Error: Missing folder\n" + usage_info)
        sys.exit(-2)
    output_folder = args[0]
    for opt, arg in opts:
        if opt == '-h':
            print(usage_info)
            sys.exit()
        if opt in ("-s", "--size"):
            file_size = arg
    print(file_size)
    print(output_folder)
    return int(file_size), output_folder


if __name__ == '__main__':
    main(*read_args(sys.argv))
