#!/bin/bash

##Handle killing parallel processes
trap stop INT
function stop() {
	  pkill -f netsort
	  echo "\nEnded processes"
}

##Remove old netsort executable
rm -f netsort
rm nohup.out

##Build netsort
go build -o netsort netsort.go

##Run for process
for i in $(seq 0 3)
do
  SERVER_ID=$i
  INPUT_FILE_PATH='testcases/testcase1/input-'${SERVER_ID}'.dat'
  OUTPUT_FILE_PATH='testcases/testcase1/output-'${SERVER_ID}'.dat'
  CONFIG_FILE_PATH='testcases/testcase1/config.yaml'
  nohup ./netsort ${SERVER_ID} ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH} &
done

wait
