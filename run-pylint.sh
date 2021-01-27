#!/bin/bash
set -e
export PYTHONPATH=../home/tasrif
pylint -f pylint_junit.JUnitReporter -r y ../home/ >&1 | tee pylint/report.xml
