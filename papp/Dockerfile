FROM python:3.6-alpine

COPY papp /app
COPY main.py /app
COPY requirements.txt /app
COPY data /data
WORKDIR  /app
RUN pip3 install -r requirements.txt

USER 1001
EXPOSE 8080

CMD ["python", "main.py", "8080"]