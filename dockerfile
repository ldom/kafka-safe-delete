# FROM tiangolo/meinheld-gunicorn:python3.8
FROM tiangolo/uwsgi-nginx-flask:python3.8

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

COPY ./app /app
