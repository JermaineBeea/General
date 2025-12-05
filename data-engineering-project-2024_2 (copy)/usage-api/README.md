# Usage API

A REST API service that exposes daily usage summaries from the Hypertext Variant System (HVS) for customers based on Call Detail Records (CDR). The API includes basic HTTP authentication for security.

## Overview

The Usage API provides endpoints for customers to retrieve:
- Daily aggregated CDR data (call counts, durations, etc.)
- Overall usage summaries across date ranges
- Breakdown by call type (voice vs. video)

All endpoints require basic HTTP authentication.

## Features

- **Basic Authentication**: All API endpoints (except `/health`) require username and password authentication
- **Daily Usage Summaries**: Get aggregated usage data by date for a specific MSISDN (phone number)
- **Date Range Filtering**: Query data within specific date ranges using optional parameters
- **Call Type Breakdown**: Separate metrics for voice and video calls
- **Health Check**: Endpoint to verify API and database connectivity

## API Endpoints

### 1. Health Check (No Authentication Required)

```
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-12-05T10:30:45.123456"
}
```

### 2. Get Daily Usage Summary

```
GET /api/usage/<msisdn>?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

**Authentication:** Required (Basic Auth)

**Parameters:**
- `msisdn` (path): Phone number (e.g., 7643008066273)
- `start_date` (query): Optional start date in YYYY-MM-DD format
- `end_date` (query): Optional end date in YYYY-MM-DD format

**Response:**
```json
{
  "msisdn": "7643008066273",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "total_records": 10,
  "data": [
    {
      "date": "2024-01-05",
      "total_calls": 15,
      "voice_calls": 8,
      "video_calls": 7,
      "total_duration_seconds": 12345,
      "avg_duration_seconds": 823,
      "min_duration_seconds": 50,
      "max_duration_seconds": 1800
    },
    ...
  ]
}
```

### 3. Get Overall Usage Summary

```
GET /api/usage/<msisdn>/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

**Authentication:** Required (Basic Auth)

**Parameters:**
- `msisdn` (path): Phone number
- `start_date` (query): Optional start date in YYYY-MM-DD format
- `end_date` (query): Optional end date in YYYY-MM-DD format

**Response:**
```json
{
  "msisdn": "7643008066273",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "summary": {
    "msisdn": "7643008066273",
    "total_calls": 150,
    "voice_calls": 85,
    "video_calls": 65,
    "total_duration_seconds": 123450,
    "avg_duration_seconds": 823,
    "active_days": 25,
    "first_call": "2024-01-01T10:05:46.916465",
    "last_call": "2024-01-31T22:15:35.668864"
  }
}
```

## Authentication

The API uses **HTTP Basic Authentication**. When making requests, include the `Authorization` header:

```
Authorization: Basic base64(username:password)
```

### Default Credentials

- **Username**: `admin`
- **Password**: `admin123`

These can be changed via environment variables.

### Example Using cURL

```bash
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273

# Or with date range
curl -u admin:admin123 "http://localhost:5000/api/usage/7643008066273?start_date=2024-01-01&end_date=2024-01-31"

# Get summary
curl -u admin:admin123 "http://localhost:5000/api/usage/7643008066273/summary"
```

### Example Using Python requests

```python
import requests
from requests.auth import HTTPBasicAuth

url = "http://localhost:5000/api/usage/7643008066273"
auth = HTTPBasicAuth('admin', 'admin123')

response = requests.get(url, auth=auth)
data = response.json()
print(data)
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `postgres` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `postgres` | Database username |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_NAME` | `wtc_analytics` | Database name |
| `API_USERNAME` | `admin` | API basic auth username |
| `API_PASSWORD` | `admin123` | API basic auth password |
| `PORT` | `5000` | API port |

## Database Schema

The API queries the `cdr_data.voice_cdr` table:

```sql
CREATE TABLE cdr_data.voice_cdr (
    msisdn VARCHAR(20) NOT NULL,
    tower_id INTEGER,
    call_type VARCHAR(20),           -- 'voice' or 'video'
    dest_nr VARCHAR(20),              -- Destination number
    call_duration_sec INTEGER,        -- Duration in seconds
    start_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_voice_cdr_msisdn ON cdr_data.voice_cdr(msisdn);
CREATE INDEX idx_voice_cdr_start_time ON cdr_data.voice_cdr(start_time);
```

## Error Handling

The API returns appropriate HTTP status codes:

| Status | Description |
|--------|-------------|
| 200 | Successful request |
| 401 | Authentication required or invalid credentials |
| 404 | Endpoint not found |
| 500 | Internal server error |
| 503 | Service unavailable (database connection failed) |

**Error Response:**
```json
{
  "error": "Authentication required"
}
```

## Running the Service

### Docker Compose

The service is automatically included in `docker-compose.yml`:

```bash
docker-compose up usage-api
```

### Standalone

```bash
pip install -r requirements.txt
python main.py
```

The API will be available at `http://localhost:5000`

## Testing

```bash
# Health check (no auth required)
curl http://localhost:5000/health

# Get daily usage (with auth)
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273

# Get daily usage with date range
curl -u admin:admin123 "http://localhost:5000/api/usage/7643008066273?start_date=2024-01-01&end_date=2024-01-31"

# Get summary
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273/summary
```

## Performance

The API includes database indexes on:
- `msisdn`: For fast MSISDN lookups
- `start_time`: For date range queries

Query performance is optimized for typical workloads with aggregate functions.

## Future Enhancements

- Token-based authentication (JWT)
- Rate limiting
- Caching layer
- Additional metrics (cost, data usage, etc.)
- Pagination for large result sets
- Export to CSV/JSON formats
