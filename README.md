# Prerequisites



# Execution

The code was written in Python 3 and tested under Python 3.6.5, but is (to the best of my knowledge) compliant with 
earlier versions of Python 3.x as well as with Python 2.7. 
  


## Local environment

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

# Tests

`pytest`