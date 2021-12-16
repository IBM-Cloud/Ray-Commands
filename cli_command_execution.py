import os
import subprocess

import ray
import time
import sys
import humanfriendly

url_file = open('final_commands.txt','r')

lines = url_file.readlines()
from subprocess import PIPE, Popen
#print(lines)


@ray.remote(num_cpus=0.25)
def do_it(filen, input_endpoint, output_endpoint, input_access_key, input_secret_key, output_access_key, output_secret_key):
    filen=filen.strip()
    cmd_string='mc alias set source https://{0} {1} {2} > /dev/null; mc alias set target https://{3} {4} {5} > /dev/null; {6}'.format(input_endpoint, input_access_key, input_secret_key, output_endpoint, output_access_key, output_secret_key, filen)
    print("Executing: '{}'".format(filen))
    p = Popen(cmd_string, shell=True, stdout=PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    output = stdout.decode("utf-8")
    print("'%s'" % output)
    transferred = output[output.find("Transferred: "):output.find("Speed: ")].split(" ")
    transferred_str = transferred[1] + transferred[2]
    return(transferred_str)
    #print("Transferred: {}".format(humanfriendly.parse_size(transferred_str)))

if __name__ == '__main__':
    ray.init(address='auto')

    all_ref_objs = []

    total_tasks = len(lines)
    for task_number, line in enumerate(lines, start=1):
        print("\033[96m(Main loop)\033[0m Submitting task {}/{} for: {}".format(task_number, total_tasks, line))
        refobj = do_it.remote(line.strip(), sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
        all_ref_objs.append(refobj)

    start_seconds = time.time()
    result_ids = all_ref_objs
    start_time = time.time()
    while len(result_ids) > 0:
        done_id, result_ids = ray.wait(result_ids)
        now_seconds = time.time()
        duration=now_seconds-start_seconds
        print("\033[96m(Main loop)\033[0m Tasks left: {}/{}".format(len(result_ids), len(all_ref_objs)))
        print("\033[96m(Main loop)\033[0m Average tasks/second: ", ((len(all_ref_objs)-len(result_ids))*1.1)/duration)
    end_time=time.time()

    print("\033[96m(Main loop)\033[0m Total time (s): ", end_time-start_time)


