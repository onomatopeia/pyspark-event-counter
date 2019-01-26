import os
import random
import time
from datetime import timedelta, date
from math import ceil


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
        return f"\"{self.latitude():.5f}\",\"{self.longitude():.5f}\",\"{self.region()}\",\"{self.date()}\"\n"

    def events(self, n):
        for _ in range(n):
            yield self.event()


avg_chars_per_line = 42


def generate_events(path_to_file, file_size=1073741824, regions=53, start_date=date.today(), date_range=7):
    """This method will by default generate a file of approximately 1GB with fake event data."""
    fake = FakeNZEventGenerator(start_date, date_range, regions)
    t0 = time.time()

    n = int(file_size // avg_chars_per_line)
    with open(path_to_file, "w+") as f:
        f.write("\"lat\",\"long\",\"region_id\",\"date\"\n")
        f.write("".join(fake.events(n)))

    t1 = time.time()
    return t1 - t0, n


def generate_events_in_chunks(path_to_file, file_size=1073741824, regions=53, start_date=date.today(), date_range=7,
                              chunk_size = 23):
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
    return t1 - t0, N


def test_generate_events():
    events_file = os.path.join('data', 'test_location.csv')
    try:
        t, n = generate_events(events_file, 1024)
        file_size = os.path.getsize(events_file)
        print(f"{n} fake events generated in {t} seconds and written to {events_file} [{file_size}]")
        assert n == 24, f"Expected 24 events but got {n}"
        assert file_size < 1.1 * 1024, f"Expected approx 1024 B but got {file_size}B"
    finally:
        try:
            os.remove(events_file)
        except FileNotFoundError:
            pass


if __name__ == '__main__':
    output_file = os.path.join('data', 'location2.csv')
    T, N = generate_events_in_chunks(output_file)
    output_file_size = os.path.getsize(output_file)
    print(f"{N} fake events generated in {T} seconds and written to {output_file} [{output_file_size}]")

