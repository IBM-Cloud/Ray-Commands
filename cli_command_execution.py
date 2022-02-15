import os
import subprocess

import ray
import time
import sys
import humanfriendly
import pandas as pd

url_file = open('final_commands.txt','r')

lines = url_file.readlines()
from subprocess import PIPE, Popen

@ray.remote(num_cpus=0.2)
def do_it(filen, input_endpoint, output_endpoint, input_access_key, input_secret_key, output_access_key, output_secret_key):
    filen=filen.strip()
    cmd_string='mc alias set source https://{0} {1} {2} > /dev/null; mc alias set target https://{3} {4} {5} > /dev/null; {6}'.format(input_endpoint, input_access_key, input_secret_key, output_endpoint, output_access_key, output_secret_key, filen)
    #print("Executing: '{}'".format(filen))
    def run_command(cmd_string):
        p = Popen(cmd_string, shell=True, stdout=PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = p.communicate()
        return stdout.decode("utf-8")
    max_attempts=3
    attempt=1
    while attempt < max_attempts:
        output = run_command(cmd_string)
        #print("'%s'" % output)
        try:
            transferred = output[output.find("Transferred: "):output.find("Speed: ")].split(" ")
            transferred_str = transferred[1] + transferred[2]
            return(transferred_str)
        except IndexError:
            attempt+=1 # Try again
    print("Couldn't parse transferred data from output of copy task: {}".format(filen))
    print("Output was:\n{}".format(output))
    print("Returning 0 for transferred data volume.")
    return "0"

def submission_progress(tasks, tasks_total):
    bar_len = 80
    tasks_filled_len = int(round(bar_len * tasks / float(tasks_total)))
    tasks_percents = round(100.0 * tasks / float(tasks_total), 1)
    tasks_bar = '=' * tasks_filled_len + '-' * (bar_len - tasks_filled_len)
    sys.stdout.write('Tasks submitted (%s/%s) [%s] %s%s  \r' %
                     (tasks, tasks_total, tasks_bar, tasks_percents, '%'))
    sys.stdout.flush()

def execution_progress(bytes, bytes_total, tasks_left, tasks_total, duration):
    bar_len = 25
    bytes_filled_len = int(round(bar_len * bytes / float(bytes_total)))
    bytes_percents = round(100.0 * bytes / float(bytes_total), 1)
    bytes_bar = '=' * bytes_filled_len + '-' * (bar_len - bytes_filled_len)
    tasks_filled_len = int(round(bar_len * (tasks_total-tasks_left) / float(tasks_total)))
    tasks_percents = round(100.0 * (tasks_total-tasks_left) / float(tasks_total), 1)
    tasks_bar = '=' * tasks_filled_len + '-' * (bar_len - tasks_filled_len)
    sys.stdout.write('Bytes(%s/%s = %s/s) [%s] %s%s Tasks(%s/%s) [%s] %s%s Time(s):%s  \r' %
        (humanfriendly.format_size(bytes, binary=True), humanfriendly.format_size(bytes_total, binary=True),
         humanfriendly.format_size(round(bytes/duration), binary=True), bytes_bar, bytes_percents, '%',
         tasks_total-tasks_left, tasks_total, tasks_bar, tasks_percents, '%', round(duration)))
    sys.stdout.flush()

if __name__ == '__main__':
    ray.init(address='auto')

    all_ref_objs = []

    total_tasks = len(lines)
    for task_number, line in enumerate(lines, start=1):
        #print("\033[96m(Main loop)\033[0m Submitting task {}/{} for: {}".format(task_number, total_tasks, line))
        refobj = do_it.remote(line.strip(), sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
        all_ref_objs.append(refobj)
        submission_progress(task_number, total_tasks)

    print("")
    total_volume = pd.read_csv("object_2_size.csv")["size"].apply(humanfriendly.parse_size).sum()

    start_seconds = time.time()
    result_ids = all_ref_objs
    transferred_volume=0
    while len(result_ids) > 0:
        done_ids, result_ids = ray.wait(result_ids)
        for done_id in done_ids:
            transferred_volume+=1.048913043478261*humanfriendly.parse_size(ray.get(done_id))
        #print("\033[96m(Main loop)\033[0m Transferred: {} / {}".format(humanfriendly.format_size(transferred_volume, binary=True),
        #                                                               humanfriendly.format_size(total_volume, binary=True)))
        duration=time.time()-start_seconds
        #print("\033[96m(Main loop)\033[0m Elapsed time (s): {}".format(duration))
        #print("\033[96m(Main loop)\033[0m Tasks left: {} / {}".format(len(result_ids), len(all_ref_objs)))
        #print("\033[96m(Main loop)\033[0m Average Tasks/s: ", ((len(all_ref_objs)-len(result_ids))*1.1)/duration)
        #print("\033[96m(Main loop)\033[0m Average Bytes/s: ", humanfriendly.format_size(transferred_volume/duration, binary=True))
        execution_progress(transferred_volume, total_volume, len(result_ids), len(all_ref_objs), duration)

    total_time=time.time()-start_seconds
    print("\033[96m(Main loop)\033[0m Total time (s): ", total_time)
    print("\033[96m(Main loop)\033[0m Total average Bytes/s: ", humanfriendly.format_size(total_volume/total_time, binary=True))


