FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y python3 python3-pip python-is-python3 && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /myapp
COPY my_python_app.py /myapp/
WORKDIR /myapp
CMD ["python", "my_python_app.py"]
