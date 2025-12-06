import os
from datetime import datetime
from functools import wraps
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configuration from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "database": os.getenv("DB_NAME", "wtc_analytics")
}

API_USERNAME = os.getenv("API_USERNAME", "admin")
API_PASSWORD = os.getenv("API_PASSWORD", "admin123")


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def require_auth(f):
    """Decorator for basic authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != API_USERNAME or auth.password != API_PASSWORD:
            return jsonify({"error": "Authentication required"}), 401, {
                "WWW-Authenticate": 'Basic realm="Login Required"'
            }
        return f(*args, **kwargs)
    return decorated


def build_date_filter(params):
    """Build date filter SQL and parameters."""
    conditions = []
    values = []
    
    if params.get("start_date"):
        conditions.append("DATE(start_time) >= %s")
        values.append(params["start_date"])
    
    if params.get("end_date"):
        conditions.append("DATE(start_time) <= %s")
        values.append(params["end_date"])
    
    return conditions, values


def execute_query(query, params):
    """Execute query and return results."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/api/usage/<msisdn>", methods=["GET"])
@require_auth
def get_usage_summary(msisdn):
    """Get daily usage summary for an MSISDN."""
    try:
        date_conditions, date_params = build_date_filter(request.args)
        
        query = """
            SELECT 
                DATE(start_time) as date,
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                AVG(call_duration_sec) as avg_duration_seconds
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """
        
        if date_conditions:
            query += " AND " + " AND ".join(date_conditions)
        
        query += " GROUP BY DATE(start_time) ORDER BY date DESC"
        
        results = execute_query(query, [msisdn] + date_params)
        
        return jsonify({
            "msisdn": msisdn,
            "start_date": request.args.get("start_date"),
            "end_date": request.args.get("end_date"),
            "data": [dict(row) for row in results]
        })
        
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/usage/<msisdn>/summary", methods=["GET"])
@require_auth
def get_usage_aggregate(msisdn):
    """Get overall usage summary for an MSISDN."""
    try:
        date_conditions, date_params = build_date_filter(request.args)
        
        query = """
            SELECT 
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                COUNT(DISTINCT DATE(start_time)) as active_days,
                MIN(start_time) as first_call,
                MAX(start_time) as last_call
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """
        
        if date_conditions:
            query += " AND " + " AND ".join(date_conditions)
        
        results = execute_query(query, [msisdn] + date_params)
        
        return jsonify({
            "msisdn": msisdn,
            "start_date": request.args.get("start_date"),
            "end_date": request.args.get("end_date"),
            "summary": dict(results[0]) if results else {}
        })
        
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)