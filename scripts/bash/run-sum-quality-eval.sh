#!/bin/bash

while getopts ":a:d:o:xl" opt; do
  case $opt in
  a)
    algorithm="$OPTARG"
    ;;
  d)
    dataPath="$OPTARG"
    ;;
  o)
    operator="$OPTARG"
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

#errorLevel="0 0.00001 0.00005 0.0001 0.0005 0.001 0.005 0.01"
errorLevel="0 0.0001"

timeout=600

#for i in $(seq 0 0.005 1); do
for i in $errorLevel; do
#  echo "Used error level: $i"
  if [ "$algorithm" = 'Aggrdet' ]; then
    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
    --local-scheduler \
    --algorithm $algorithm \
    --dataset-path $dataPath \
    --error-level "$i" \
    --target-aggregation-type $operator \
    --use-extend-strategy $extendStrategy \
    --use-delayed-bruteforce $delayBruteForceStrategy \
    --eval-only-aggor False \
    --log-level WARNING \
    --error-strategy ratio \
    --timeout $timeout
  elif [ "$algorithm" = 'Baseline' ]; then
    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
    --local-scheduler \
    --algorithm $algorithm \
    --dataset-path $dataPath \
    --error-level "$i" \
    --eval-only-aggor False \
    --log-level WARNING \
    --timeout $timeout
  else
    :
  fi
done

#for i in $errorLevel; do
##  echo "Used error level: $i"
#  if [ "$algorithm" = 'Aggrdet' ]; then
#    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
#    --local-scheduler \
#    --algorithm $algorithm \
#    --dataset-path $dataPath \
#    --error-level "$i" \
#    --use-extend-strategy $extendStrategy \
#    --use-delayed-bruteforce $delayBruteForceStrategy \
#    --eval-only-aggor True \
#    --log-level WARNING \
#    --error-strategy ratio \
#    --timeout $timeout
#  elif [ "$algorithm" = 'Baseline' ]; then
#    env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
#    --local-scheduler \
#    --algorithm $algorithm \
#    --dataset-path $dataPath \
#    --error-level "$i" \
#    --eval-only-aggor True \
#    --log-level WARNING \
#    --timeout $timeout
#  else
#    :
#  fi
#done

#qualityEvalResultPath="${outputPath}quality-eval/"
#echo $qualityEvalResultPath
#mkdir -p $qualityEvalResultPath

#find ${outputPath}error-level-*/ -name "quality-eval-error-level*" -exec cp {} $qualityEvalResultPath \;
