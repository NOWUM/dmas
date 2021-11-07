FROM python:3.10-slim

COPY requirements_mrk.txt .

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements_mrk.txt
RUN useradd -s /bin/bash admin

ENV TZ="Europe/Berlin"

RUN mkdir /src
RUN chown -R admin /src

# COPY ./model /src

COPY ./model/agents/__init__.py /src/agents/__init__.py
COPY ./model/agents/basic_Agent.py /src/agents/basic_Agent.py
COPY ./model/agents/mrk_Agent.py /src/agents/mrk_Agent.py

COPY ./model/apps/__init__.py /src/apps/__init__.py
COPY ./model/apps/market.py /src/apps/market.py

COPY ./model/interfaces /src/interfaces
COPY ./model/main_mrk.py /src/main_mrk.py

USER admin
WORKDIR /src

CMD ["python", "./main_mrk.py"]