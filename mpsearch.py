# -----------------------------------------------------------------------------
# file: mpsearch.py
# brief: This script allows a specified string in a stream of data to be searched by 10 simultaneous processes.
# -----------------------------------------------------------------------------

import os
import re
import sys
import time
import math
import random
import logging
import argparse
from multiprocessing import Process, Array, RLock, Queue

# -----------------------------------------------------------------------------
# Sort the child processes by [elapsed] field

def sort_by_elapsed(stat):
    return stat[1]

# -----------------------------------------------------------------------------
# Report specific exception and status of the child process to stdout

def exception_handler(pid, logger):
    logger.warning("No work has been assigned to worker %s \n" %pid)
    status = 'FAILURE'
    return status    

# -----------------------------------------------------------------------------
# Write a summary to stdout
 
def write_report(status_workers, num_workers, logger):
    total_elapsed_time = 0
    total_bytes_read = 0
    workers_complete_in_time = []

    for worker_success in status_workers[0]:
        pid, elapsed, byte_cnt, status = worker_success
        elapsed_in_ms = elapsed * 1000   # new line added 
        total_elapsed_time += elapsed_in_ms
        total_bytes_read += byte_cnt
        workers_complete_in_time.append(pid)
        logger.info("{0} {1} {2} {3}\n".format(pid, elapsed_in_ms, byte_cnt, status))
            
    for worker_failed in status_workers[-1]:
        pid, elapsed, byte_cnt, status = worker_failed
        logger.info("{0} {1} {2} {3}\n".format(pid, elapsed, byte_cnt, status))
        workers_complete_in_time.append(pid)
        
    for pid in range(num_workers):
        if not pid in workers_complete_in_time:
             logger.info("{0} {1} {2} {3}\n".format(pid, None, None, 'TIMEOUT'))  # report TIMEOUT
        
    avg_time_per_bytes_read = total_elapsed_time / total_bytes_read
    logger.info("The average time spent per byte read in ms: " + str(avg_time_per_bytes_read))

# -----------------------------------------------------------------------------
# Assign work to child processes

def search_string(pid, array, rlock, q, path, total_bytes, workload_per_worker, logger, target_string):
    rlock.acquire()
    start_position = max(array)
    end_position = array[pid] = (start_position + workload_per_worker) \
                    if (start_position + workload_per_worker) <= total_bytes else total_bytes
    rlock.release()
    
    elapsed_time = None
    bytes_read = None
    status = None

    if start_position == total_bytes:
        try:
            raise EOFError
        except Exception:
            status = exception_handler(pid, logger)

    elif start_position < total_bytes:
        data_stream = open(path, 'rb')
        productivity_min = 3*len(target_string)
        productivity_max = 6*len(target_string)
        productivity = random.randint(productivity_min, productivity_max)   # create an integer as the worker's read rate
        current_position = start_position
        data_stream.seek(start_position)
        pattern = re.compile(target_string)
        target_found = None
        start_time = time.time()   # start timing
            
        while not target_found and current_position < end_position:
            data_read = data_stream.read(productivity)
            target_found = pattern.search(str(data_read))
            current_position += productivity

        if target_found or (current_position >= end_position):
            elapsed_time = time.time() - start_time   # end timing
            bytes_read = current_position - start_position
            status = 'SUCCESS'
        data_stream.close()
            
    q.put((pid, elapsed_time, bytes_read, status))   # put data as a tuple to the shared queue
    

def main():
    
    print("*******************************************************")
    print("*                    Code Exercise                    *")
    print("*                     Cheng Peng                      *")
    print("*******************************************************")
    
    
    current_directory = os.getcwd()
    logging.basicConfig(level=logging.INFO, filename=os.path.join(current_directory, 'report.log'), \
                        filemode='w', format="%(message)s")
    logger = logging.getLogger()
    
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(levelname)-7s %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    # -----------------------------------------------------------------------------
    # Parse arguments
    
    parser = argparse.ArgumentParser(description="Search for a string in a data stream by deploying 10 workers.")
    parser.add_argument('path', help="requires a file path to start the work")
    parser.add_argument('string', help="requires a specified string want to search")
    parser.add_argument('timeout', default=60, nargs='?', help="'timeout' in second (60s by default)")
    
    args = parser.parse_args()
    path = args.path
    string = args.string
    timeout = float(args.timeout)
    
    if not os.path.exists(path):
        try:
            raise FileNotFoundError
        except Exception:
            logger.error("Failed to open the data stream.")
            sys.exit(1)
    
    total_bytes = os.path.getsize(path)
    if total_bytes == 0:
        try:
            raise ValueError
        except Exception:
            logger.error("The data stream is empty.")
            sys.exit(1)
    
    # -----------------------------------------------------------------------------
    # Deploy child processes for searching the string 'xAd'
    
    num_workers = 10
    workload_per_worker = math.ceil(total_bytes / num_workers)   # assign a block of data to a child process
    rlock = RLock()
    array = Array('i', num_workers, lock=rlock)   # shared object that records a data block's end position of a child process
    q = Queue()   # shared queue for communication between parent and child processes
    
    workers = [Process(target=search_string, args=(i, array, rlock, q, path, total_bytes, workload_per_worker, logger, string)) \
               for i in range(num_workers)]
    
    for i in range(num_workers):
        workers[i].start()
        
    for j in range(num_workers):
        workers[j].join(timeout)   # block at most timeout second
    
    for k in range(num_workers):
        if workers[k].is_alive():
            workers[k].terminate()
    
    # -----------------------------------------------------------------------------
    # Get and process the data from the shared queue
    
    status_workers = [[], []]
    while not q.empty(): 
        stat = q.get()
        if stat[1] != None:
            status_workers[0].append(stat)
        else:
            status_workers[-1].append(stat)
            
    status_workers[0].sort(key=sort_by_elapsed, reverse=True)
    write_report(status_workers, num_workers, logger)
    
# -----------------------------------------------------------------------------
# Run script

if __name__ == '__main__':
    main()

