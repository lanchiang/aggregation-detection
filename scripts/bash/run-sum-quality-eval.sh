#!/bin/bash

while getopts ":a:d:xl" opt; do
  case $opt in
  a)
    algorithm="$OPTARG"
    ;;
  d)
    dataPath="$OPTARG"
    ;;
  l)
    delayBruteForceStrategy=true
    ;;
  x)
    extendStrategy=true
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

projectRootPath="../../"

pythonPath="${projectRootPath}aggrdet"
#echo $pythonPath

if [ "$extendStrategy" != true ]; then
  extendStrategy=false
else
  extendStrategy=true
fi
#echo "Use extended aggregation strategy: $extendStrategy"

if [ "$delayBruteForceStrategy" != true ]; then
  delayBruteForceStrategy=false
#else
#  delayBruteForceStrategy=true
fi

echo "$delayBruteForceStrategy"

errorLevel="0 0.00001 0.00003 0.00005 0.0001 0.0003 0.0005 0.001 0.003 0.005 0.01 0.03 0.05"
#errorLevel="0.003 0.005 0.01 0.03 0.05"
#errorLevel="0.0001"

#for i in $(seq 0 0.005 1); do
for i in $errorLevel; do
#  echo "Used error level: $i"
  if [ "$algorithm" = 'Aggrdet' ]; then
    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
    --local-scheduler \
    --algorithm $algorithm \
    --dataset-path $dataPath \
    --error-level "$i" \
    --use-extend-strategy $extendStrategy \
    --use-delayed-bruteforce $delayBruteForceStrategy \
    --log-level WARNING \
    --error-strategy ratio
  elif [ "$algorithm" = 'Baseline' ]; then
    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
    --local-scheduler \
    --algorithm $algorithm \
    --dataset-path $dataPath \
    --error-level "$i" \
    --log-level WARNING
  else
    :
  fi
done

#qualityEvalResultPath="${outputPath}quality-eval/"
#echo $qualityEvalResultPath
#mkdir -p $qualityEvalResultPath

#find ${outputPath}error-level-*/ -name "quality-eval-error-level*" -exec cp {} $qualityEvalResultPath \;
