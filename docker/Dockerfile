FROM python:3.8
COPY requirements.txt .
RUN pip3 install --no-cache -r requirements.txt \
 && apt-get update && apt-get install -y jq atool && apt-get clean
