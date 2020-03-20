#!/usr/bin/env python
import requests
import time
import datetime
from elasticsearch import Elasticsearch
import numpy
#import thread
import threading
import random
import string
import json
import yaml



working_threads = 8
hits_per_thread = 5000
host_es = "192.168.5.237"
index_name = "test_data"
batch_size = 500

with open('bench-configuration.yml', 'r') as f:
	doc = yaml.load(f)
	working_threads = doc["write_module"]["number_of_threads"]

	hits_per_thread = doc["write_module"]["hits_per_thread"]
	division= doc["write_module"]["division_report"]

	report_time = hits_per_thread/division

	host_es = doc["general"]["es_host"]
	index_name = doc["general"]["index"]
	batch_size = doc["write_module"]["batch_size"]
	timeout_value = doc["write_module"]["timeout"]


# This function hits with "hits_per_thread" the system
def hit_es( threadNum, times):
    time_outs = 0 
    #connect to our cluster
    es = Elasticsearch([{'host': host_es, 'port': 9200}])

    upload_data_txt = ""
    upload_data_count = 0
   
    with open("./finalLogsDataSet") as f:
            artLogs = f.readlines()

    for i in range(hits_per_thread):
        if i%100000==0:
            print "On the Way! " + str(i)
        item = random.choice(artLogs)

        cmd = {'index': {'_index': index_name,
                          '_type': 'nova9'}}
        


        upload_data_txt += json.dumps(cmd) + "\n"

        upload_data_txt += item
        upload_data_count += 1

       # print upload_data_txt

        if upload_data_count == batch_size:
            start_time = time.time()
            
            while True:
                try:
                    res = es.bulk(index = index_name, body = upload_data_txt, refresh = False, timeout= timeout_value)
                except:
                    print "Connection time-out occured. Consider a bigger time-out limit"
                    time_outs = time_outs + 1
                    continue
                break
            
            res_txt = "OK" if not res['errors'] else "FAILED"


            #print (res_txt)
            finish_time = (time.time() - start_time)
            #print finish_time
            real_time = res['took']
            #print (real_time)
            upload_data_txt = ""
            upload_data_count = 0
            times.append(real_time)
        #print result['hits']['total']
    print ("Thread " + str(threadNum) + " finished... \n\n\n")
    print " Total time-outs: " + str(time_outs)


class myThread (threading.Thread):
    def __init__(self, threadID, name, timeList):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.timeList = timeList
    def run(self):
        print ("Starting " + self.name)
        hit_es(self.threadID, self.timeList)
        print ("Exiting " + self.name)


times = []
threads = []

overall_start_time = time.time()

# Create and start the threads
for thread_id in range(working_threads):
    # Create threads as follows
    print ("Creating thread " + str(thread_id) + "..." )
    # Create new thread
    newThread = myThread(thread_id, "Thread-"+str(thread_id), times)
    # Start new Thread
    newThread.start()
    # Add thread to thread list
    threads.append(newThread)


# Wait for all threads to complete
for t in threads:
    t.join()

print ("Exiting Main Thread...")
print ("My list has length: " + str(len(times)) )


# Calculate statistics
overall_time = time.time() - overall_start_time 
no_queries = hits_per_thread * working_threads
throughPut = no_queries/overall_time


print ("Overall time: " + str(overall_time))
print ("ThroughPut : " + str(no_queries/overall_time) + "(servedQueries/sec)")



print ("\n\nFinished with querries with the below statistics:")

avg_time =  numpy.mean(times)

#put_settings(*args, **kwargs)

es = Elasticsearch([{'host': host_es, 'port': 9200, }])
health = es.cluster.health(index=index_name)
data_nodes = health['number_of_data_nodes']
active_primary_shards = health['active_primary_shards']

avg_per_doc = avg_time/batch_size
print ("Average time per bulk: " + str(avg_time) + " ms")
print ("Average per doc: " + str(avg_per_doc) + " ms" )
print ("Cluster:" + health['cluster_name'])
print ("Status:" + health['status'])
print ("Number of data nodes:" + str(data_nodes))
print ("Number of active_primary_shards:" + str(active_primary_shards))




line_to_write = str(data_nodes) + " " + str(avg_time)
# write the results into the final file so as to plot them.
with open("write_stats.txt", "a") as text_file:
    text_file.write(line_to_write)
    text_file.write("\n")
