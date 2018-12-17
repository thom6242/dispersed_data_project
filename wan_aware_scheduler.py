#!/usr/bin/python
import itertools
import numpy as np
import pandas as pd

zones = ['us-east1-b','asia-south1-c','europe-west2-c']
zone_combs = []

num_of_zones_req = 2

data = np.array([['','us-east1-b','asia-south1-c','europe-west2-c'],
                ['us-east1-b',1000,50,70],
                ['asia-south1-c',50,1000,20],
		['europe-west2-c',70,20,1000]])

zone_WAN_info = (pd.DataFrame(data=data[1:,1:],
                  index=data[1:,0],
                  columns=data[0,1:]))

def getTotalWAN(zones):
	total_sum = 0
	for comb in itertools.combinations(zones,2):
		total_sum += int(zone_WAN_info[comb[0]][comb[1]])

	return total_sum

def get_top_k_zones():

	zones_with_WAN_info = []

	for zone_comb in itertools.combinations(zones,num_of_zones_req):
		zone_combs.append(list(zone_comb))
		zones_with_WAN_info.append((getTotalWAN(list(zone_comb)),list(zone_comb)))

	zones_with_WAN_info.sort(reverse=True)

	return zones_with_WAN_info[0][1]

print get_top_k_zones()
