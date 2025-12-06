FROM python:3.11-slim
WORKDIR /dbt
RUN pip install dbt-postgres==1.7.0
COPY . .
CMD ["tail", "-f", "/dev/null"]