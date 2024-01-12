FROM python:3.11
ENV PYTHONUNBUFFERED True
ENV PYTHONPATH /

WORKDIR /
COPY ./requirements /requirements

RUN pip install --no-cache-dir --upgrade -r /requirements/pip_requirements.txt

COPY . /

ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8080"]
