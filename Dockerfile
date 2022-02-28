FROM python:3.7
LABEL maintainer="Ummar Abbas <uabbas@hbku.edu.qa>"

WORKDIR /home

# upgrade pip itself
RUN pip3 install --upgrade pip

COPY qa-requirements.txt /home
RUN pip3 install -r qa-requirements.txt

# copy common requirements to cache the dependencies
COPY requirements.txt /home/requirements.txt
RUN MINIMAL_KATS=1 pip3 install -r requirements.txt

# install tasrif and its dependencies in editable mode
COPY setup.py /home/setup.py
COPY README.md /home/README.md
RUN pip3 install --use-deprecated=legacy-resolver -e .

COPY run-pylint.sh /home
COPY run-darglint.sh /home

# copy quick start example files
COPY examples/quick_start /home/examples/quick_start

COPY / /home
