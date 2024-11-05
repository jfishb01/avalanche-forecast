FROM python:3.12
ENV PYTHONUNBUFFERED True
ENV PYTHONPATH /
ENV DAGSTER_HOME=/opt/app

WORKDIR /

COPY requirements /requirements
RUN apt-get update
RUN xargs -a /requirements/apt_requirements.txt apt-get install -y
RUN pip install --no-cache-dir --upgrade -r /requirements/pip_requirements.txt

COPY pyproject.toml /opt/app/pyproject.toml
COPY config/prod/dagster.yaml /opt/app/dagster.yaml
COPY src /opt/app/src

WORKDIR /opt/app

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "12000", "--python-file", "src/__init__.py"]
