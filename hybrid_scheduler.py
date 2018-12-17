#!/usr/bin/env python
import time
import random
import json
from kubernetes import client, config, watch
from collections import defaultdict
from sdcclient import SdcClient
config.load_kube_config()
v1 = client.CoreV1Api()
sdclient = SdcClient("d27bca02-23c6-44f0-b1db-6b48fe177585")
#sysdig_metric = "net.http.request.time"
sysdig_metric = "cpu.used.percent"
#metrics = [{ "id": sysdig_metric, "aggregations": { "time": "timeAvg", "group": "avg" } }]
metrics = [{ "id": "cpu.used.percent", "aggregations": { "time": "timeAvg", "group": "avg" } }]
scheduler_name = "hybrid"

zone_node_pod = defaultdict()
zone_pod = defaultdict()


def nodes_available():
    for n in v1.list_node().items:
        for status in n.status.conditions:
            if status.status == "True" and status.type == "Ready" and n.spec.taints is None:
                zone = n.metadata.labels["zone"]
		node = n.metadata.name
		if zone not in zone_node_pod.keys():
		# add to dictionary that stores zone and nodes info along with pod count on each node
			zone_node_pod[zone] = {node:0}
			zone_pod[zone] = 0
		else:
			zone_node_pod[zone].update({node:0})
	

def is_balanced(pods_map):
	prev = -1
	for k in pods_map.keys():
		pods = pods_map[k]
		if prev != -1 and pods != prev:
			return False
		prev = pods
	return True

	
def get_next(pods_map):
	for k in pods_map.keys():
		pods_map[k] += 1
		return k


def get_imbalanced(pods_map):
	avg = 0.0
	for k in pods_map.keys():
		avg += pods_map[k]
	avg = avg/len(pods_map)
	for k in pods_map.keys():
		pods = pods_map[k]
		if pods < avg:
			pods_map[k] += 1
			return k
	
	
def get_optimal_node():
	if is_balanced(zone_pod):
		zone = get_next(zone_pod)
		return best_request_time(zone_node_pod[zone].keys())
	zone = get_imbalanced(zone_pod)
	return best_request_time(zone_node_pod[zone].keys())


def get_request_time(hostname):
    hostfilter = "host.hostName = '%s'" % hostname
    start = -60
    end = 0
    sampling = 60
    #import pdb
    #pdb.set_trace()
    metricdata = sdclient.get_data(metrics, start, end, sampling, filter=hostfilter)
    #import pdb
    #pdb.set_trace()
    request_time = float(metricdata[1].get('data')[0].get('d')[0])
    print hostname + " (" + sysdig_metric + "): " + str(request_time)
    return request_time


def best_request_time(nodes):
    #time.sleep(20)
    if not nodes:
        return []
    node_times = [get_request_time(hostname) for hostname in nodes]
    best_node = nodes[node_times.index(min(node_times))]
    print "Best node: " + best_node
    time.sleep(20)
    return best_node



def scheduler(name, node, namespace="default"):
    body=client.V1Binding()
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node
    meta=client.V1ObjectMeta()
    meta.name=name
    body.target=target
    body.metadata=meta
    return v1.create_namespaced_binding_binding(name,namespace, body)


def main():
    w = watch.Watch()
    nodes_available()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name:
            try:
		res = scheduler(event['object'].metadata.name, get_optimal_node())
            except client.rest.ApiException as e:
                pass


if __name__ == '__main__':
    main()     

