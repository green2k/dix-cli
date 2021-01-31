#!/bin/bash

workdir="dix_cli"
a=0;
echo "----------------------------------------------------------------------------------"
echo "Running isort checks"
isort --check-only -df
a+=$?;
echo "----------------------------------------------------------------------------------"
echo "Running prospector checks"
prospector $workdir
a+=$?;
echo "----------------------------------------------------------------------------------"
echo "Running mypy checks"
mypy $workdir
a+=$?;
echo "----------------------------------------------------------------------------------"
if [ $a -ne 0 ]; then
  echo "Lint checks failed!"
  exit 1
fi
echo "Lint checks passed!"
