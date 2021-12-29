FROM python:3.9-slim

ENV TZ="Europe/Berlin"

# Switch to root for install
USER root

# install glpk
# add coinor-cbc if needed
RUN apt-get update && apt-get install --no-install-recommends -y gcc g++ libglpk-dev glpk-utils\
   && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -s /bin/bash admin

RUN mkdir /src
RUN mkdir -p /home/admin
RUN chown -R admin /src
RUN chown -R admin /home/admin

COPY ./model /src

USER admin
WORKDIR /src

CMD ["python", "-u" ,"./main.py"]
