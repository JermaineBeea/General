Week 6 practical instruction
Contents
Getting started
Basic docker commands
Questions
Task 1:
Task 2:
Task 3:
Task 4:
In this practical you will get some hands-on experience working with containers and Docker:

You will spin up a basic ubuntu container, install python on it and build an image of the new container

You will create a Dockerfile, build an image from it and create and package a python application

You will containerise an already-build application, run the application and rebuild a new version of the image

You will create a containerised application that allows for environment variables and command line arguments to be passed in at runtime

Getting started
Install Docker

To experiment with Docker if not installed, use Docker Labs

Warning: Do not use any passwords in this VM

Basic docker commands
Command

Description

docker –help

All available commands

docker pull ubuntu:20.04

Pull ubuntu 20.04 image to your local repository

docker build . -t myimage

Build Dockerfile in current directory (.) and name it myimage

docker run

-i, –interactive

-t

-p

Run an image

Allows you to provide input to the container’s STDIN

Creates a psuedo-terminal to work in the container

Binds the host port to the containers port

docker ps

-a

View all running container(s)

View all containers that have run/are running and their exit statuses

docker images or docker image ls

View all images

docker commit

Create a new image from a container’s changes

docker diff

Lists all changes in container, compared to image

docker rm or

Remove the container

docker rmi

Remove the image

Questions
Task 1:
Spin up a basic ubuntu container

Install python on that container

Build an image of the new container

1.1) Run a base ubuntu image and name it ubuntu-v1:

docker run -it --name ubuntu-1 ubuntu

You may notice that your command prompt has changed.

This means you are now running within the terminal of the running container (because we specified -t).

root@cb47090bec15 :/#
1.2) Check whether python is available in the container (we can interact with the container because we specified -i):

root@<container_id>:/# python3 --version
It is not available (command not found).

1.3) Install python on this container:

root@<container_id>:/# apt-get update
root@<container_id>:/# apt-get install python3
Ubuntu uses an Advanced Package Tool (APT) to manage installation and removal of software. This package manager is used on various Debian flavoured distributions. Before applying updates to the working operating system (install), you need to refresh the package repository with apt-get update.

Check that python is now installed:

root@<container_id>:/# python3 --version
This time it should exist.

1.4) Open a new terminal window and check that you can see the container:

docker ps
You should see that the container is up and running:

CONTAINER ID

IMAGE

COMMAND

CREATED

STATUS

PORTS

NAMES

E98bab4f00c4

ubuntu

"/bin/bash"

7 minutes ago

Up 7 minutes

ubuntu-1

1.5) Return to your container terminal window and exit the container:

exit
1.6) Return to your second window and rerun docker ps

You will notice that the container is not running anymore. This is because containers are ephemeral.

1.7) To view all containers that have run/are running and their exit statuses, run:

docker ps -a
1.8) Rerun the following command to spawn another container from the ubuntu image:

docker run -it --name ubuntu-2 ubuntu
You will notice that it does not include python (python3 --version ). This is because we installed python on the container’s writable layer, but the image it was spawned from doesn’t include python.

1.9) List all the changes made to your first container where you installed python (when compared to the image):

docker diff <container_id or container_name>
1.10) Build a new image from the container where you installed python:

docker commit <container_id/name> ubuntu_with_python:1
The ‘1’ is a tag

1.11) View all your images:

docker images
You should see the newly created image:

REPOSITORY

TAG

IMAGE ID

CREATED

SIZE

Ubuntu_with_python

1

fccd38e25f56

5 seconds ago

138 MB

TAKEAWAY: You can pull any pre-built image and spawn a container from it. You can make changes to that container (which do not affect the base image) and build a new image from that container that you can spin up containers from in the future.

Task 2:
Create a Dockerfile

Build an image from the Dockerfile

Create a python application

Package the python application

2.1) Create a new folder and Dockerfile:

mkdir -p task2/ubuntu-python
cd task2/ubuntu-python
echo "FROM ubuntu:latest
RUN apt-get update
RUN apt-get install python3 -y
RUN rm -rf /var/lib/apt/lists/*
" > Dockerfile
Here we are specifying a new image that will be created from the ubuntu base image and install our python dependencies as the next layers in the filesystem. The rm instruction is an autoremove, autoclean command to reduce the layer size (a Dockerfile best practice).

2.2) Build the image from the Dockerfile:

docker build . -t my_python_image
Running docker images, you should see your image has been built. Using our previous command we can spawn a new container from the image and check that it has python installed

docker run -it my_python_image
root@<container_id>:/# python3 --version
root@<container_id>:/# exit
2.3) To view information about the Docker image and layers, run:

docker history my_python_image
You will see that the image has 8 layers - 5 that make up the base image, and 1 new layer for each command in the Dockerfile

2.4) Change your Dockerfile to execute all the RUN commands in a single command:

FROM ubuntu:latest
RUN apt-get update \
&& apt-get install python -y \
&& rm -rf /var/lib/apt/lists/*
2.5) Build a new image and spawn a container:

docker build . -t my_python_image2
docker run my_python_image2
2.6) View the image history:

docker history my_python_image2
The layers have been reduced to 6 - the 5 original layers, and 1 additional layer for the combined RUN command in the Dockerfile. The image size is reduced as a result from 139MB to 109MB ( docker images )

2.7) Create a simple python app that prints hello world:

cd ..
mkdir python-app
echo 'print("hello world")' > ./python-app/my_python_app.py
2.8) Create a Dockerfile and configure it in such a manner that it executes your application on startup

echo "FROM ubuntu:latest
RUN apt-get update && apt-get install python3 -y \
&& rm -rf /var/lib/apt/lists/*
RUN mkdir ./myapp
COPY my_python_app.py /myapp
WORKDIR /myapp
CMD python3 ./my_python_app.py
" > python-app/Dockerfile
Docker is spawning a container from the my_python_image and the line CMD python3 ./my_python_app.py assures that upon creation, our application is executed. Once that process terminates, the container terminates.

2.9) Build and run the image again:

docker build ./python-app -t my_python_image
docker run my_python_image
Note: Use docker images or docker image ls to list all your images

TAKEAWAY Now you have built a docker image from a Dockerfile that is hosted on your local docker repository, that contains your packaged application. You can now ship your image by pushing it to DockerHub (if you were to do this, you will need an account). It may then be served or delivered to a platform like Kubernetes.

Task 3:
Pull an already built application

Build the image

Run the application

Build a new version with changes

Remove the container

Remove your images

3.1) Clone the week 6 starter repository for the test-app and list the contents:

mkdir -p task3
cd task3
git clone git@gitlab.wethinkco.de:starters/data-engineering-week-6.git
cd data-engineering-week-6
This repository contains the following:

Dockerfile

requirements.txt

index.py

3.2) Build the image from the Dockerfile and run an instance of the image as a container:

docker build . -t hellodocker:v1
3.3) Run the image, exposing it on port 5000:

docker run -it -p 5000:5000 hellodocker:v1
You should be able to see the output in the browser at: http://localhost:5000

3.4) Kill the container

ctrl+c
3.5) Change the index.py file to:

print out the value of 2 to the power of 10 (instead of hello world)

3.6) Rebuild the image with a different version tag:

docker build . -t hellodocker:v2
3.7) Run that container

You should see the changes in the browser at the same address.

3.8) To remove the container, first find the container_id or container_name (docker ps -a), then run:

docker rm <container_name> or <container_id>
3.9) To remove the image, find the image_id (docker images / docker image ls), then run:

docker rmi <image_id>
TAKEAWAY You can expose a port on the container that can give you access to any of the services running in that container. You can rebuild images of your packaged applications using tags for versioning.

Task 4:
Build a Dockerfile using an alpine base image - pass the image tag alpine3.6 at build time (hint: build ARGS)

Install the relevant software in order run index.py

Create a Dockerfile and python application (modified index.py ) that prints out 2*x where you provide x:

as an environment variable at runtime (hint: os.environ), specifying a default value of 5 in your Dockerfile - build this image

as a command line argument when spinning up the container - build this image

Create an account on Dockerhub and ship the above 2 images (hint: docker push)

Questions:
Complete the above instructions

Submit your image tags, Dockerfiles and python scripts to a gitlab that you create so you can share for review.

TAKEAWAY Build arguments can be passed in at build time to bake state into your images. You can create containerised applications that allow environment and/or command line arguments to be passed in at runtime. Environment variables are always inherited from the environment in which the container is spun up. You can ship your built images to Dockerhub where they are available to your team.