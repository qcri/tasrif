import sys
from setuptools import setup, find_packages


print(find_packages(), file=sys.stderr)

setup(name='tasrif', version='0.1', packages=find_packages())
