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

# Dependancy conflict between tsfresh==0.18.0 and ray==1.7.0
# where tsfresh requires matrixprofile which requires protobuf==3.11.1
# and ray requires protobuf>=3.15.1. 
# Therefore matrixprofile version listed below requires protobuf>=3.15.1
MATRIXPROFILE_VERSION='matrixprofile @ git+https://github.com/abalhomaid/matrixprofile.git' + \
                                    '@f7a6788cae2267af129c9c6ad813e0375f37c321'

setup(
    name='tasrif',
    version='0.1',
    packages=find_packages(),
    python_requires='>= 3.7',
    install_requires=[
        'pandas >= 1.1.1',
        NUMPY_VERSION,
        MATRIXPROFILE_VERSION,
        'pyjq >= 2.5.1',
        'ummalqura >= 2.0.1',
        'scikit-learn >= 0.22.1',
        'tqdm >= 4.52.0',
        'tsfresh >= 0.18.0',
        'kats @ https://github.com/facebookresearch/kats/archive/eefa85275d36ec50e1fed3864f9bd7a435a3b405.zip',
        'ray >= 1.7.0'
    ],
    # numpy also needs to be specified in setup_requires,
    # see https://github.com/numpy/numpy/issues/2434#issuecomment-65252402
    setup_requires=[
        NUMPY_VERSION,
    ],
)
