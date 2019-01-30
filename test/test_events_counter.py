from __future__ import print_function
import events_counter


def test_parse_argv():
    args = 'events_counter.py -d s3://folder'.split()

    dir, delete = events_counter.parse_argv(args)

    assert dir == 's3://folder', 'Expected s3://folder but got {}'.format(dir)
    assert delete is True, 'Expected True for deletion but got False'
