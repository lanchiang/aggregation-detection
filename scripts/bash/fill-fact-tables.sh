#!/bin/bash

while getopts ":d:n:" opt; do
  case $opt in
  d)
    dataPath="$OPTARG"
    ;;
  n)
    datasetName="$OPTARG"
    ;;
  \?)
    echo "Invalid option: -$OPTARG" >&2
    exit 1
    ;;
  :)
    echo "Option -$OPTARG requires an argument." >&2
    exit 1
    ;;
  esac
done

#echo $dataPath
#echo $datasetName

cd ../../aggrdet || exit
env PYTHONPATH=. luigi --module database UploadDatasetDB --local-scheduler --log-level WARNING --dataset-path $dataPath --dataset-name $datasetName