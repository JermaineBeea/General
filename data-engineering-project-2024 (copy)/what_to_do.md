Based on your curriculum and issues, here's my recommendation:

## Issue Priority Order

**Do ISSUE 1 first** (Forex pipeline to Redpanda), then ISSUE 2 (Usage API), and finally ISSUE 3 (CI/CD).

Here's why and what to do for each:

---

## ISSUE 1: Forex Pipeline to Redpanda
**Priority: 1st** - This is foundational infrastructure

### What you need to do:
1. **Verify forex data is producing correctly**
   - The `forex` container already produces tick data to the `tick-data` topic in Redpanda
   - Check Redpanda Console at `http://localhost:18084` to confirm data is flowing

2. **Create a consumer service**
   - Build a Python service that consumes from the `tick-data` topic
   - Store the data in PostgreSQL `wtc_analytics.forex_data` schema

3. **Create database table**
   ```sql
   CREATE TABLE wtc_analytics.forex_data.forex_raw (
       timestamp TIMESTAMP,
       pair_name VARCHAR(10),
       bid_price DECIMAL(10,4),
       ask_price DECIMAL(10,4),
       spread DECIMAL(10,4)
   );
   ```

4. **Key implementation steps**:
   - Create a new folder like `forex-consumer/`
   - Write Python code using `kafka-python` to consume messages
   - Insert records into PostgreSQL using `psycopg2` or `sqlalchemy`
   - Add error handling and logging
   - Containerize it (Dockerfile)
   - Add to `docker-compose.yml`

---

## ISSUE 2: Usage API
**Priority: 2nd** - Depends on having processed CDR data

### What you need to do:
1. **First, complete TBD 3 & TBD 4** (prerequisites):
   - Set up stream processing for CDR data (consume from Redpanda, aggregate by MSISDN)
   - Deploy a High Velocity Store (HVS) like ScyllaDB or use PostgreSQL as alternative
   - Store summarized usage data

2. **Build REST API**:
   - Use Flask or FastAPI framework
   - Implement endpoint: `GET /data_usage?msisdn=XXX&start_time=YYY&end_time=ZZZ`
   - Add basic authentication (username/password)
   - Query HVS for the requested MSISDN and time range
   - Return JSON in the format specified in curriculum

3. **Response format**:
   ```json
   {
     "msisdn": "2712345678",
     "start_time": "...",
     "end_time": "...",
     "usage": [
       {
         "category": "data",
         "usage_type": "video",
         "total": 12312323,
         "measure": "bytes",
         "start_time": "2024-01-01 00:00:00"
       }
     ]
   }
   ```

---

## ISSUE 3: CI/CD Pipeline
**Priority: 3rd** - Do this last once core functionality works

### What you need to do:
1. **Create a CI/CD workflow** (GitHub Actions, GitLab CI, or Jenkins)

2. **Pipeline stages**:
   ```yaml
   stages:
     - install_dependencies
     - run_unit_tests
     - validate_pipelines
     - build_docker_images
     - deploy_system
   ```

3. **For each service, create**:
   - `requirements.txt` or `setup.py`
   - Unit tests using `pytest`
   - Test deployment scripts
   - Docker build configurations

4. **Scripts needed**:
   - `setup_venv.sh` - Create virtual environments
   - `install_deps.sh` - Install requirements
   - `run_tests.sh` - Execute unit tests
   - `deploy.sh` - Deploy via docker-compose

---

## Quick Start Recommendation

Start with **ISSUE 1** by creating this structure:

```
data-engineering-project-2024/
├── forex-consumer/
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── tests/
│       └── test_consumer.py
```

Then update `docker-compose.yml` to include your new forex-consumer service, similar to how the `cdr` and `crm` services are configured.

Would you like me to help you write the code for the forex consumer service?
