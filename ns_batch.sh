#!/bin/bash

#일별 데이터
nohup /usr/local/bin/python3.9 /DATA/landinfo/nsdailybatch/python/ns_batch_job.py daily > /dev/null &
#전체 데이터
nohup /usr/local/bin/python3.9 /DATA/landinfo/nsdailybatch/python/ns_batch_job.py full > /dev/null &
#지적도만 전체
nohup /usr/local/bin/python3.9 /DATA/landinfo/nsdailybatch/python/ns_batch_job.py jijuk_all > /dev/null &
