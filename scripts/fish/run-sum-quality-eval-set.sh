#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -p parameterA"
   echo -p "\t-p The port used to establish database connection."
   exit 1 # Exit script after printing help
}

while getopts ":p:i" opt
do
   case "$opt" in
      i) initDB=true;;
      p) port="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

if [ "$initDB" = true ]; then
  echo 'Initialize database...'
  sh init-database.sh -p $port
fi

# run aggrdet
sh ./run-sum-quality-eval.sh -a Aggrdet -d ../../data/troy.jl.gz
sh ./run-sum-quality-eval.sh -x -a Aggrdet -d ../../data/troy.jl.gz

# run baseline
sh ./run-sum-quality-eval.sh -a Baseline -d ../../data/troy.jl.gz