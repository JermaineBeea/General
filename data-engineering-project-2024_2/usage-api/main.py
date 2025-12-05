"""
Usage API - REST API for retrieving daily usage summaries
Provides endpoints to query CDR data (Call Detail Records) for a given MSISDN
with basic authentication support.
"""

import os
from datetime import datetime
from functools import wraps

import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configuration
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "wtc_analytics")

# Basic Auth credentials (can be loaded from environment variables)
API_USERNAME = os.getenv("API_USERNAME", "admin")
API_PASSWORD = os.getenv("API_PASSWORD", "admin123")


def get_db_connection():
    """Create a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except psycopg2.Error as e:
        app.logger.error(f"Database connection error: {e}")
        raise


def check_auth(username, password):
    """Check if username and password are valid."""
    return username == API_USERNAME and password == API_PASSWORD


def authenticate():
    """Authenticate function for basic auth."""
    return jsonify({"error": "Authentication required"}), 401, {
        "WWW-Authenticate": 'Basic realm="Login Required"'
    }


def require_auth(f):
    """Decorator to require basic authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated_function


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint (no authentication required)."""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/api/usage/<msisdn>", methods=["GET"])
@require_auth
def get_usage_summary(msisdn):
    """
    Get daily usage summary for a given MSISDN.
    
    Query Parameters:
    - start_date: Start date (YYYY-MM-DD) - optional
    - end_date: End date (YYYY-MM-DD) - optional
    
    Returns:
    - Daily aggregated CDR data including call counts, duration, and costs
    """
    try:
        # Get optional date parameters
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build query
        query = """
            SELECT 
                DATE(start_time) as date,
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                AVG(call_duration_sec) as avg_duration_seconds,
                MIN(call_duration_sec) as min_duration_seconds,
                MAX(call_duration_sec) as max_duration_seconds
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """
        
        params = [msisdn]
        
        if start_date:
            query += " AND DATE(start_time) >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND DATE(start_time) <= %s"
            params.append(end_date)
        
        query += " GROUP BY DATE(start_time) ORDER BY date DESC"
        
        cur.execute(query, params)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        if not results:
            return jsonify({
                "msisdn": msisdn,
                "data": [],
                "message": "No usage data found for this MSISDN"
            }), 200
        
        # Convert results to list of dicts
        data = []
        for row in results:
            data.append(dict(row))
        
        return jsonify({
            "msisdn": msisdn,
            "start_date": start_date,
            "end_date": end_date,
            "total_records": len(data),
            "data": data
        }), 200
        
    except psycopg2.Error as e:
        app.logger.error(f"Database error: {e}")
        return jsonify({"error": "Database query failed"}), 500
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/usage/<msisdn>/summary", methods=["GET"])
@require_auth
def get_usage_aggregate(msisdn):
    """
    Get overall usage summary for a given MSISDN.
    
    Query Parameters:
    - start_date: Start date (YYYY-MM-DD) - optional
    - end_date: End date (YYYY-MM-DD) - optional
    
    Returns:
    - Aggregated CDR data across the date range
    """
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                %s as msisdn,
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                AVG(call_duration_sec) as avg_duration_seconds,
                COUNT(DISTINCT DATE(start_time)) as active_days,
                MIN(start_time) as first_call,
                MAX(start_time) as last_call
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """
        
        params = [msisdn, msisdn]
        
        if start_date:
            query += " AND DATE(start_time) >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND DATE(start_time) <= %s"
            params.append(end_date)
        
        cur.execute(query, params)
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if not result:
            return jsonify({
                "msisdn": msisdn,
                "message": "No usage data found for this MSISDN"
            }), 200
        
        return jsonify({
            "msisdn": msisdn,
            "start_date": start_date,
            "end_date": end_date,
            "summary": dict(result)
        }), 200
        
    except psycopg2.Error as e:
        app.logger.error(f"Database error: {e}")
        return jsonify({"error": "Database query failed"}), 500
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
