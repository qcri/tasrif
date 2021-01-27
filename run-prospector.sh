#!/bin/bash
set -e
export PYTHONPATH=../home/tasrif
prospector --output-format grouped >&1 | tee prospector/prospector-report.txt
