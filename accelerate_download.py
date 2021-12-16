import asyncio
#import concurrent.futures
import requests
import os
import sys
import ray
import time
import datetime
import pandas as pd
#import plots
import plots_cos
#from plots_cos import create_execution_histogram, create_rates_histogram, create_agg_bdwth_plot


#url_file = open('/root/warc.urls.sub','r')
#url_file = open('/root/warc.paths.url1000','r')
#url_file = open('/root/cos_files.txt','r')
# !!!! COMMent in the line below for getting urls pointing to aws s3, otherwise the one below is for IBM COS
url_file = open('./warc.paths.url10000','r')
#url_file = open('./urls_cos.txt','r')


lines = url_file.readlines()
print("lines: ", lines)


OUTPUT='/dev/null'

def create_plots(res_write, res_read, outdir, name, details_string):
    create_execution_histogram(res_write, res_read, "{}/{}_execution.png".format(outdir, name+details_string))
    create_rates_histogram(res_write, res_read, "{}/{}_rates.png".format(outdir, name+details_string))
    create_agg_bdwth_plot(res_write, res_read, "{}/{}_agg_bdwth.png".format(outdir, name+details_string))

def get_size(url):
    print("getting size for url: ", url)
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size

@ray.remote(num_cpus=1.0)
def download_range(url, start, end, output):
#    print("edl, ", url)

#!!!! COMMENTED OUT FOR NON_CHUNKING
#    headers = {'Range': f'bytes={start}-{end}'}

    start_time= time.time()

#!!!! COMMENTED OUT FOR NON_CHUNKING
#    response = requests.get(url, headers=headers)
    response = requests.get(url)
#    with open(output, 'wb') as f:
    with open(os.devnull, 'wb') as f:
#        for part in response.iter_content(100400000):
#            f.write(part)
             f.write(response.content)
    end_time=time.time()
    total_time=end_time-start_time
#    time_elapsed_seconds=time_elapsed.total_seconds()
#!!!! COMMENTED OUT FOR NON_CHUNKING
#    return {'start_time': start_time, 'total_time': total_time, 'mb_rate': (end-start)/total_time/1e6, 'bytes_read': end-start, 'end_time': end_time}
    return {'start_time': start_time, 'total_time': total_time, 'mb_rate': 11000/total_time/1e6, 'bytes_read': 1100000000, 'end_time': end_time}
#    return {'worker_start_tstamp' : start_time, 'worker_end_tstamp': end_time, 'total_time': time_elapsed}


#the download version is with chunking. there is another one without chunking
#def download(url, output, chunk_size=100000000):
#    print("entering download: ", url)
#    file_size = get_size(url)
#    chunks = range(0, file_size, chunk_size)

 #   ref_objs = []

 #   for i, start in enumerate(chunks):
 #       ref_obj = download_range.remote(url, start, start + chunk_size - 1, output + url[-13:] + str(i))
 #       ref_objs.append(ref_obj)

#    print("exiting download: ", url)
 #   return ref_objs

def download(url, output, chunk_size=100000000):
    print("entering download: ", url)
#    file_size = get_size(url)
#    chunks = range(0, file_size, chunk_size)

    ref_objs = []

 #   for i, start in enumerate(chunks):
    start = 0
    chunk_size=1
    i=1
    ref_obj = download_range.remote(url, start, start + chunk_size - 1, output + url[-13:] + str(i))
  #      ref_objs.append(ref_obj)

    print("exiting download: ", url)
    return ref_objs



if __name__ == '__main__':

#    ray.init()
    ray.init(address='auto')
#    ray.init(address='141.125.161.24:6379', _redis_password='5241590000000000')

#    executor = concurrent.futures.ThreadPoolExecutor(max_workers=240)
 #   loop = asyncio.get_event_loop()
#    chunk_size=sys.argv[1]

    all_ref_objs = []

 #   for url in URLS:
 #       ref_objs = download(url, OUTPUT,int(chunk_size))
 #       all_ref_objs.extend(ref_objs)
    start_time = time.time()
    for url in lines:
       print("URL", url)
       ref_objs = download(url.strip(), OUTPUT)
       all_ref_objs.extend(ref_objs)

#    print("refs: ", len(all_ref_objs))
#    start_time= datetime.datetime.now()
    print("start: ", start_time)
    start_dt  = datetime.datetime.today()
    start_seconds = start_dt.timestamp()
 
    result_ids=all_ref_objs
#    total_chunks=len(result_ids)
    time_elapsed_seconds_each = []
    while len(result_ids) > 0:
        done_id, result_ids = ray.wait(result_ids)
        now_dt  = datetime.datetime.today()
        now_seconds = now_dt.timestamp()
        duration=now_seconds-start_seconds
        print("time: ", time.time())
        print("resultids", len(result_ids))
        print("Rate: ", ((len(all_ref_objs)-len(result_ids))*int(chunk_size)/1e9)/duration)

        print("allrefobjects: ", len(all_ref_objs))
        print("len(result_ids)", len(result_ids))
        print("duration: ", duration)
        print("chunk_size", chunk_size)
#        print("2")
    end_time=datetime.datetime.now()
#    print("total chunks: " , total_chunks)
  #  print(chunk_size)
#    ichunks=int(total_chunks)
 #   itotal=int(chunk_size)
   # total_data_size=int(total_chunks)*int(chunk_size)
  #  print("total data size: ", total_data_size)
  #  print("end time: ", end_time.strftime("%Y-%m-%d-%H-%M-%S-%f"))


    all_timing_data = ray.get(all_ref_objs)


    worker_status=[]
    counter=0
    for r in all_timing_data:
        print("r", r)
        worker_status.append({'worker_start_tstamp' : r['start_time'], 'worker_end_tstamp' : r['start_time'] + r['total_time']})
        
    benchmark_stats= {
        'start_time' :  start_time,
        'results': all_timing_data,
        'worker_stats' : worker_status}

    print("aaa", benchmark_stats['results'])

    df = pd.DataFrame(benchmark_stats['results'])
    print("before-file")
    filepath='./0.xlsx'
    print("abefore-file")
#    df.to_excel(filepath, index=False)

    print("all timing data: ", benchmark_stats)
    
#    plots_cos.create_execution_histogram(benchmark_stats,benchmark_stats,"~/ex.png")
#    plots_cos.create_rates_histogram(benchmark_stats, benchmark_stats, "~/rates.png")
    plots_cos.create_agg_bdwth_plot(benchmark_stats, benchmark_stats, "~/agg_bdwth.png")
#    plots.create_rates_histogram(benchmark_stats,"~/tmp")
#        time.sleep(1)
  #      for result_id in result_ids:
   #         print(time_elapsed_seconds_each)
    #        time_elapsed_seconds_each.append(ray.get(result_id))

#        print("time elapsed: ", time_elapsed_seconds_each)
 #       total_size_copied=(len(total_chunks)-len(result_ids))*int(chunk_size)

  #      print("remaining  IDs: ", len(result_ids))
   #     dt  = datetime.datetime.today()
    #    seconds = dt.timestamp()
     #   print(str(seconds), " Done-IDs: ", len(done_id))

  #      print("Copied so far in GB: ", (total_size_copied/1e9), "time: ", str(seconds))
   #     current_time=datetime.datetime.now()
    #    time_elapsed=current_time-start_time
     #   print("time elapsed: ", time_elapsed.total_seconds())
#     gb_rate=float((total_data_size/1e9))/float(time_elapsed.total_seconds())
#     print("GB rate until now: ", gb_rate)
   #     time.sleep(3)
#
    print("time elapsed seconds each: ", time_elapsed_seconds_each)
#    ready_refs, remaining_refs = ray.wait(return_object_refs, num_returns=2, timeout=None)
    print("3")

