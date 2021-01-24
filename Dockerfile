FROM python:3.6.5
LABEL maintainer="Ummar Abbas <uabbas@hbku.edu.qa>"

ARG worker
WORKDIR /home

# upgrade pip itself
RUN pip3 install --upgrade pip

COPY qa-requirements.txt /home
RUN pip3 install -r qa-requirements.txt

# copy common requirements to cache the dependencies
COPY requirements.txt /home/requirements.txt
RUN pip3 install -r requirements.txt

COPY run-prospector.sh /home
COPY run-pylint.sh /home

COPY / /home
