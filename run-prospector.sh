#!/bin/bash
set -e
prospector --output-format grouped >&1 | tee prospector/prospector-report.txt
