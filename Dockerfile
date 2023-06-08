FROM python:3.11

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y apt-transport-https ca-certificates dirmngr \
    && GNUPGHOME=$(mktemp -d) \
    && GNUPGHOME="$GNUPGHOME" gpg --no-default-keyring --keyring /usr/share/keyrings/clickhouse-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8919F6BD2B48D754 \
    && rm -r "$GNUPGHOME" \
    && chmod +r /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4 \
    && apt-get update \
    && apt-get install -y clickhouse-client

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install .
