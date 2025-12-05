FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# install minimal system deps, upgrade pip, and install Python deps in one layer
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# copy only the application entrypoint (keeps image small)
COPY main.py .

EXPOSE 5000

CMD ["python", "main.py"]
