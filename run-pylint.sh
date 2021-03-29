#!/bin/bash
set -e
set -o pipefail # Prevents pipes from swallowing errors

mkdir -p pylint
pylint -f pylint_junit.JUnitReporter ../home/tasrif | tee pylint/report.xml
