# MultipleSearch
Multiple processes programming exercise.

#### Run

python version 3.0+

$python [mpsearch.py] [path of data stream] [string] [timeout]


#### Output

Output is stored in the current working directory.

The output file name: report.log

#### Tests

###### Edge Case

   The input stream bytes are less than the number of processes (0 < data <10 bytes)
   Output: some processes could ’SUCCESS’, others may have a warning and a ’FAILURE’ status.


###### Error Case

   The input is an invalid path of data stream / an empty data file (0 byte)
   Output: cause an error in console, report.log.
