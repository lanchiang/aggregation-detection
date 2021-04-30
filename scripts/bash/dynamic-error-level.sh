#!/bin/bash

projectRootPath="../../"

error_level_candidates_path='../../aggrdet/temp/error_level_dicts.txt'

pythonPath="${projectRootPath}aggrdet"

while IFS= read -r line; do
  echo "Parameter setting: $line"
  env PYTHONPATH=$pythonPath luigi --module evaluation QualityEvaluation \
    --local-scheduler \
    --algorithm Aggrdet \
    --dataset-path ../../data/dataset.jl.gz \
    --error-level "$line" \
    --log-level WARNING
done < $error_level_candidates_path