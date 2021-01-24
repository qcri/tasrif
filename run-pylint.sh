#!/bin/bash
set -e
pylint -f pylint_junit.JUnitReporter -r y ../home/ >&1 | tee pylint/report.xml
