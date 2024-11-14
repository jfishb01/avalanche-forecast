FROM python:3.12
ENV PYTHONUNBUFFERED True
ENV PYTHONPATH /
ENV DAGSTER_HOME=/opt/app

WORKDIR /

# Install DuckDB CLI
RUN mkdir /duckdb-install && \
    wget https://artifacts.duckdb.org/latest/duckdb-binaries-linux.zip -O /duckdb-install/duckdb.zip && \
    unzip /duckdb-install/duckdb.zip -d /duckdb-install && \
    unzip /duckdb-install/duckdb_cli-linux-amd64.zip -d /usr/local/bin && \
    rm -rf /duckdb-install

# Install apt and pip requirements
COPY requirements /requirements
RUN apt-get update && xargs -a /requirements/apt_requirements.txt apt-get install -y
RUN pip install --no-cache-dir --upgrade -r /requirements/pip_requirements.txt

# Copy and set up application directories
COPY pyproject.toml /opt/app/pyproject.toml
COPY config/prod/dagster.yaml /opt/app/dagster.yaml
COPY src /opt/app/src

WORKDIR /opt/app

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "12000", "--python-file", "src/__init__.py"]
