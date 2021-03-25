#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -p parameterA"
   echo -p "\t-p The port used to establish database connection."
   exit 1 # Exit script after printing help
}

while getopts ":p:" opt
do
   case "$opt" in
      p ) port="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# create the database
sh create-database.sh $port

# load the fact tables with datasets
#sh fill-fact-tables.sh -d ../data/troy.jl.gz -n troy
sh fill-fact-tables.sh -d ../data/euses.jl.gz -n euses