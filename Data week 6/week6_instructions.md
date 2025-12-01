I'll guide you through completing Week 6 step by step. This practical covers Docker containers, images, and Dockerfiles.

## Prerequisites
- Install Docker Desktop (or use Docker Labs if not installed)
- Create a GitLab account for submission
- Create a DockerHub account for Task 4

---

## **Task 1: Basic Ubuntu Container with Python**

**Step 1.1-1.3:** Run and modify a container
```bash
# Pull and run ubuntu container
docker run -it --name ubuntu-1 ubuntu

# Inside the container, install python
apt-get update
apt-get install python3

# Verify installation
python3 --version
```

**Step 1.4-1.7:** Check container status
```bash
# Open new terminal, view running containers
docker ps

# Go back to container terminal and exit
exit

# Check all containers (including stopped)
docker ps -a
```

**Step 1.8-1.11:** Create image from modified container
```bash
# Run another container (won't have python)
docker run -it --name ubuntu-2 ubuntu
python3 --version  # Will fail
exit

# Check changes made to first container
docker diff ubuntu-1

# Create new image from modified container
docker commit ubuntu-1 ubuntu_with_python:1

# View all images
docker images
```

---

## **Task 2: Dockerfile and Python Application**

**Step 2.1-2.2:** Create Dockerfile
```bash
# Create directory structure
mkdir -p task2/ubuntu-python
cd task2/ubuntu-python

# Create Dockerfile
cat > Dockerfile << 'EOF'
FROM ubuntu:latest
RUN apt-get update
RUN apt-get install python3 -y
RUN rm -rf /var/lib/apt/lists/*
EOF

# Build image
docker build . -t my_python_image

# Test it
docker run -it my_python_image
python3 --version
exit
```

**Step 2.3-2.6:** Optimize Dockerfile layers
```bash
# View image layers
docker history my_python_image

# Update Dockerfile to combine RUN commands
cat > Dockerfile << 'EOF'
FROM ubuntu:latest
RUN apt-get update \
    && apt-get install python3 -y \
    && rm -rf /var/lib/apt/lists/*
EOF

# Build optimized image
docker build . -t my_python_image2

# Compare sizes
docker images
docker history my_python_image2
```

**Step 2.7-2.9:** Create Python application
```bash
# Go back and create app directory
cd ..
mkdir python-app

# Create simple Python app
echo 'print("hello world")' > ./python-app/my_python_app.py

# Create Dockerfile for the app
cat > python-app/Dockerfile << 'EOF'
FROM ubuntu:latest
RUN apt-get update && apt-get install python3 -y \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir ./myapp
COPY my_python_app.py /myapp
WORKDIR /myapp
CMD python3 ./my_python_app.py
EOF

# Build and run
docker build ./python-app -t my_python_image
docker run my_python_image
```

---

## **Task 3: Working with Existing Application**

**Step 3.1-3.2:** Clone and build
```bash
# Create directory and clone repo
mkdir -p task3
cd task3
git clone git@gitlab.wethinkco.de:starters/data-engineering-week-6.git
cd data-engineering-week-6

# Build the image
docker build . -t hellodocker:v1
```

**Step 3.3-3.4:** Run and test
```bash
# Run container with port mapping
docker run -it -p 5000:5000 hellodocker:v1

# Open browser to http://localhost:5000
# Press Ctrl+C to stop
```

**Step 3.5-3.7:** Modify and rebuild
```bash
# Edit index.py to calculate 2^10
# Change the print statement to: print(2**10)

# Rebuild with new version
docker build . -t hellodocker:v2

# Run new version
docker run -it -p 5000:5000 hellodocker:v2
```

**Step 3.8-3.9:** Cleanup
```bash
# Find and remove containers
docker ps -a
docker rm <container_name>

# Find and remove images
docker images
docker rmi <image_id>
```

---

## **Task 4: Advanced Dockerfile Features**

### **Part 1: Alpine base with build args**

```bash
mkdir -p task4
cd task4
```

Create `Dockerfile.alpine`:
```dockerfile
ARG ALPINE_VERSION=3.6
FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache python3
COPY index.py /app/
WORKDIR /app
CMD ["python3", "index.py"]
```

Build:
```bash
docker build -f Dockerfile.alpine --build-arg ALPINE_VERSION=3.6 -t myapp:alpine .
```

### **Part 2: Environment variable version**

Create `index_env.py`:
```python
import os

x = int(os.environ.get('X_VALUE', '5'))
result = 2 * x
print(f"2 * {x} = {result}")
```

Create `Dockerfile.env`:
```dockerfile
FROM alpine:3.6
RUN apk add --no-cache python3
ENV X_VALUE=5
COPY index_env.py /app/index.py
WORKDIR /app
CMD ["python3", "index.py"]
```

Build and run:
```bash
docker build -f Dockerfile.env -t myapp:env .
docker run myapp:env
docker run -e X_VALUE=10 myapp:env
```

### **Part 3: Command line argument version**

Create `index_arg.py`:
```python
import sys

x = int(sys.argv[1]) if len(sys.argv) > 1 else 5
result = 2 * x
print(f"2 * {x} = {result}")
```

Create `Dockerfile.arg`:
```dockerfile
FROM alpine:3.6
RUN apk add --no-cache python3
COPY index_arg.py /app/index.py
WORKDIR /app
ENTRYPOINT ["python3", "index.py"]
CMD ["5"]
```

Build and run:
```bash
docker build -f Dockerfile.arg -t myapp:arg .
docker run myapp:arg
docker run myapp:arg 15
```

### **Part 4: Push to DockerHub**

```bash
# Login to DockerHub
docker login

# Tag images with your username
docker tag myapp:env <your-username>/myapp:env
docker tag myapp:arg <your-username>/myapp:arg

# Push to DockerHub
docker push <your-username>/myapp:env
docker push <your-username>/myapp:arg
```

---

## **Submission**

Create a GitLab repository with:
```
week6-docker/
├── task1/
│   └── commands.txt (document commands used)
├── task2/
│   ├── Dockerfile
│   └── my_python_app.py
├── task3/
│   └── index.py (modified version)
└── task4/
    ├── Dockerfile.alpine
    ├── Dockerfile.env
    ├── Dockerfile.arg
    ├── index_env.py
    └── index_arg.py
```

Include a README.md with:
- Your DockerHub image tags
- Brief description of each task
- Any challenges faced

---

**Key Takeaways:**
✅ Containers are ephemeral - changes need to be committed to images
✅ Minimize Dockerfile layers for smaller images
✅ Use build args for build-time configuration
✅ Use environment variables for runtime configuration
✅ Use CMD for default args, ENTRYPOINT for fixed commands

Need help with any specific step?
