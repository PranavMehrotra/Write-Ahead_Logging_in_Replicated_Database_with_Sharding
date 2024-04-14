FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip

# Set the working directory
WORKDIR /db_server/
COPY ./requirements.txt /db_server/
RUN pip install -r requirements.txt

RUN apt-get install -y default-mysql-server

COPY . /db_server/

CMD ["bash","./db_server_start.sh"]