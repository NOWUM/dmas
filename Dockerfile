FROM python:3.8-slim

COPY requirements_mrk.txt .

RUN pip install --no-cache-dir -r requirements_mrk.txt
RUN useradd -s /bin/bash admin

ENV TZ="Europe/Berlin"

RUN mkdir /src
RUN chown -R admin /src

COPY ./model /src

# COPY ./model/agents/client_Agent.py /src/
# COPY ./model/agents/mrk_Agent.py

USER admin
WORKDIR /src


CMD ["python", "./main_mrk.py"]