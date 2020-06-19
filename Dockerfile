FROM python:3.8
COPY requirements.txt .
RUN pip3 install --no-cache -r requirements.txt \
 && apt-get update && apt-get install -y jq && apt-get clean
COPY . /bleanser
USER 1000:1000
# TODO volume??
