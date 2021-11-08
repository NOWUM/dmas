FROM python:3.8-slim

COPY requirements_dem.txt .

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements_dem.txt
# RUN pip install gurobi
RUN useradd -s /bin/bash admin

ENV TZ="Europe/Berlin"

RUN mkdir /src
RUN chown -R admin /src

COPY ./model/agents/__init__.py /src/agents/__init__.py
COPY ./model/agents/basic_Agent.py /src/agents/basic_Agent.py
COPY ./model/agents/client_Agent.py /src/agents/client_Agent.py
COPY ./model/agents/dem_Agent.py /src/agents/dem_Agent.py

COPY ./model/apps /src/apps
COPY ./model/interfaces /src/interfaces
COPY ./model/aggregation ./src/aggregation
COPY ./model/components ./src/components
COPY ./model/data ./src/data

COPY ./model/main_dem.py /src/main_dem.py

USER admin
WORKDIR /src

CMD ["python", "./main_dem.py"]