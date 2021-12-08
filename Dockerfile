FROM python:3.9-slim

ENV TZ="Europe/Berlin"

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
COPY ./model/aggregation ./src/aggregation
COPY ./model/systems ./src/systems
COPY ./model/demandlib ./src/demandlib

COPY ./model/forecasts ./src/forecasts
COPY ./model/data ./src/data

COPY ./model/main.py /src/main.py

USER admin
WORKDIR /src

CMD ["python", "-u" ,"./main.py"]