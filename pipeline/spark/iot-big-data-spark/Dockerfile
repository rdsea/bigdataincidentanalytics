FROM bde2020/spark-python-template:2.4.0-hadoop2.8

MAINTAINER Daniel Fuevesi <daniel.fuevesi@gmail.com>

COPY kmeans_sensors.py /app/app.py
COPY requirements.txt /app/requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py