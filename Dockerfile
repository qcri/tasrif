FROM python:3.7
LABEL maintainer="Ummar Abbas <uabbas@hbku.edu.qa>"

WORKDIR /home

# upgrade pip itself
RUN pip3 install --upgrade pip

COPY qa-requirements.txt /home
RUN pip3 install -r qa-requirements.txt

# copy common requirements to cache the dependencies
COPY requirements.txt /home/requirements.txt
RUN pip3 install -r requirements.txt

ARG optional_code_changed

# install tasrif and its dependencies in editable mode
COPY setup.py /home/setup.py
RUN if test "$optional_code_changed" = true ; then pip install -e .[full] ; else pip install -e . ; fi

COPY run-prospector.sh /home
COPY run-pylint.sh /home
COPY run-darglint.sh /home

COPY / /home
RUN ["chmod", "+x", "/home/run-darglint.sh"]