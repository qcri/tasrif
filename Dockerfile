FROM python:3.7 AS build
LABEL maintainer="Ummar Abbas <uabbas@hbku.edu.qa>"

ARG worker
WORKDIR /home

# upgrade pip itself
RUN pip3 install --upgrade pip

# --user option installs requirements under /root/.local/ folder
COPY qa-requirements.txt /home
RUN pip3 install --user -r qa-requirements.txt

# copy common requirements to cache the dependencies
COPY requirements.txt /home/requirements.txt
RUN pip3 install --user -r requirements.txt

# install tasrif and its dependencies in editable mode
COPY setup.py /home/setup.py
RUN pip install --user -e .

COPY run-prospector.sh /home
COPY run-pylint.sh /home
COPY run-darglint.sh /home

COPY / /home
RUN ["chmod", "+x", "/home/run-darglint.sh"]

FROM python:3.7 AS run
COPY --from=build /root/.local /root/.local
COPY --from=build /home /home

# Make sure scripts in .local are usable:
ENV PATH=/root/.local/bin:$PATH
