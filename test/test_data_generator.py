from __future__ import print_function
import os
from data_generator import generate_events, avg_chars_per_line


def test_generate_events():
    events_file = os.path.join('data', 'test_location.csv')
    try:
        t, n = generate_events(events_file, 1000)
        file_size = os.path.getsize(events_file)
        print("{} fake events generated in {} seconds and written to {} [{}]".format(n, t, events_file, file_size))
        assert n == 23, "Expected 23 events but got {}".format(n)
        expected_size = n * avg_chars_per_line
        assert file_size < 1.1 * expected_size, "Expected approx {}B but got {}B".format(expected_size, file_size)
    finally:
        try:
            os.remove(events_file)
        except FileNotFoundError:
            pass
