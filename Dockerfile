FROM python:3.8.1-alpine3.11
RUN apk update && \
pip install --upgrade pip
COPY requirements.txt /
COPY src/esmanage.py /
RUN pip install -r /requirements.txt

ENTRYPOINT [ "python","esmanage.py" ]
CMD []