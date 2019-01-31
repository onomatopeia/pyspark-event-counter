from __future__ import print_function
import events_counter


def test_parse_argv():
    args = 'events_counter.py -d s3://folder'.split()

    dir, delete = events_counter.parse_argv(args)

    assert dir == 's3://folder', 'Expected s3://folder but got {}'.format(dir)
    assert delete is True, 'Expected True for deletion but got False'

# def test_parse_long_argv():
#    args = 'spark-submit --deploy-mode cluster s3://emr-qrious-bucket/eventsQuery/events_counter.py
# s3://emr-qrious-bucket/wordcount2/data'
