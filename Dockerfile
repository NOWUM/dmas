FROM python:3.9

ENV TZ="Europe/Berlin"

# Switch to root for install
USER root

RUN apt-get update && apt-get install --no-install-recommends -y gcc g++ libglpk-dev glpk-utils

RUN wget  https://packages.gurobi.com/9.1/gurobi9.1.2_linux64.tar.gz
RUN cp gurobi9.1.2_linux64.tar.gz /tmp
RUN mkdir -p /opt && tar xfz /tmp/gurobi9.1.2_linux64.tar.gz -C /opt
ENV GUROBI_HOME /opt/gurobi912/linux64
ENV PATH $PATH:$GUROBI_HOME/bin
ENV LD_LIBRARY_PATH $GUROBI_HOME/lib
RUN cd $GUROBI_HOME && python setup.py install

COPY ./requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -s /bin/bash admin

RUN mkdir /src
RUN mkdir -p /home/admin
RUN chown -R admin /src
RUN chown -R admin /home/admin

COPY ./model/agents /src/agents
COPY ./model/apps /src/apps
COPY ./model/interfaces /src/interfaces
COPY ./model/aggregation /src/aggregation
COPY ./model/systems /src/systems
COPY ./model/demandlib /src/demandlib

COPY ./model/forecasts /src/forecasts
COPY ./model/data /src/data

COPY ./model/main.py /src/main.py

USER admin
WORKDIR /src

CMD ["python", "-u" ,"./main.py"]