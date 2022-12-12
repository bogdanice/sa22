FROM python:3.8.2-alpine

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

RUN apk add --update \
    openjdk11-jre

COPY pmodule /app/pmodule
COPY main.py /app
COPY requirements.txt /app
COPY data /data
WORKDIR  /app
RUN pip3 install -r requirements.txt

USER 1001
EXPOSE 8080

CMD ["python", "main.py", "8080"]