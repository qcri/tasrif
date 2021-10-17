"""Module to setup the library. All library dependencies are added here.
"""
import sys
import os
from setuptools import setup, find_packages

# MINIMAL=1 is to install kats minimally
os.system("export MINIMAL=1")

print(find_packages(), file=sys.stderr)

# Due to a dependency conflict with a package used by tsfresh, the numpy
# version has to be below <= 1.20.
NUMPY_VERSION='numpy <= 1.20'

setup(
    name='tasrif',
    version='0.1',
    packages=find_packages(),
    python_requires='>= 3.7',
    install_requires=[
        'pandas >= 1.1.1',
        NUMPY_VERSION,
        'pyjq >= 2.5.1',
        'ummalqura >= 2.0.1',
        'scikit-learn >= 0.22.1',
        'tqdm >= 4.52.0',
        'tsfresh >= 0.18.0',
        'kats @ https://github.com/facebookresearch/kats/archive/250b4fdd019d39bf1ae9609339dbcd595b641d2b.zip',
        'ray >= 1.7.0'
    ],
    # numpy also needs to be specified in setup_requires,
    # see https://github.com/numpy/numpy/issues/2434#issuecomment-65252402
    setup_requires=[
        NUMPY_VERSION,
    ],
)
