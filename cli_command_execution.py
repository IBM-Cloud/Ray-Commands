import os
import ray
import time
import sys

url_file = open('final_commands.txt','r')

lines = url_file.readlines()
from subprocess import PIPE, Popen
#print(lines)


@ray.remote(num_cpus=0.25)
def do_it(filen, input_endpoint, output_endpoint, input_access_key, input_secret_key, output_access_key, output_secret_key):
    print("FFF", filen)
    filen=filen.strip()
    cmd_string='mc alias set source https://{0} {1} {2}; mc alias set target https://{3} {4} {5}; {6}'.format(input_endpoint, input_access_key, input_secret_key, output_endpoint, output_access_key, output_secret_key, filen)

    p = Popen(cmd_string, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    print("stdout: '%s'" % stdout)
    print("stderr: '%s'" % stderr)


if __name__ == '__main__':
    ray.init(address='auto')

    all_ref_objs= []

    for line in lines:
        print("Launching task for: ", line)
        refobj = do_it.remote(line.strip(), sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
        all_ref_objs.append(refobj)

    start_seconds = time.time()
    result_ids=all_ref_objs
    print("resultid:", result_ids)
    start_time = time.time()
    while len(result_ids) > 0:
        done_id, result_ids = ray.wait(result_ids)
        now_seconds = time.time()
        duration=now_seconds-start_seconds
        print("duration: ", duration)
        print("resultids", len(result_ids))
        print("Rate: ", ((len(all_ref_objs)-len(result_ids))*1.1)/duration)

        print("allrefobjects: ", len(all_ref_objs))
        print("len(result_ids)", len(result_ids))
        print("duration: ", duration)
    end_time=time.time()

    print("end time: ", end_time-start_time)


