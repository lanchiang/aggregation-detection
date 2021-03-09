#!/bin/bash

while getopts ":do:x" opt; do
  case $opt in
  d) dataPath="$OPTARG";;
  x) extendStrategy=true;;
  o) outputPath="$OPTARG";;
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

projectRootPath="../../"

pythonPath="${projectRootPath}aggrdet"
#echo $pythonPath

if [[ $dataPath == "" ]]; then
  dataPath="${projectRootPath}data/troy.jl.gz"
else
  dataPath=$dataPath
fi
#echo "Input dataset path: $dataPath"

if [[ $outputPath == "" ]]; then
  outputPath="${projectRootPath}aggrdet/results/"
else
  outputPath=$outputPath
fi

if [[ $extendStrategy != true ]]; then
  extendStrategy=false
else
  extendStrategy=true
fi
#echo "Use extended aggregation strategy: $extendStrategy"

errorLevel="0 0.0001 0.0003 0.0005 0.001 0.003 0.005"
#errorLevel="0 0.1 1 5 10 20"
#errorLevel="0.1"

#for i in $(seq 0 0.005 1); do
for i in $errorLevel; do
#  echo "Used error level: $i"
  env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
  --local-scheduler \
  --dataset-path $dataPath \
  --result-path "${outputPath}error-level-${i}" \
  --error-level "$i" \
  --use-extend-strategy $extendStrategy \
  --log-level WARNING \
  --error-strategy ratio
  echo
done

qualityEvalResultPath="${outputPath}quality-eval/"
echo $qualityEvalResultPath
mkdir -p $qualityEvalResultPath

find ${outputPath}error-level-*/ -name "quality-eval-error-level*" -exec cp {} $qualityEvalResultPath \;

# store all experiment results into a postgresql database
