FROM python:3.8-buster

# Install deps
COPY src/docker/libs/requirements.txt ./requirements.txt
RUN pip install -r ./requirements.txt

# Copy code
RUN mkdir /app
COPY src/docker/src /app/src

# Create Output/Generation Directory
RUN mkdir /gen

# set working dir
RUN mkdir code
WORKDIR /code

# API it!
ENTRYPOINT ["python3", "/app/src/entrypoint.py"]
