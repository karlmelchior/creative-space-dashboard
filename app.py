from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import pytz
import time
import pyodbc
import snowflake.connector

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURATION
# =============================================================================

# DinnerBooking API
DINNERBOOKING_BASE_URL = "https://apix.dinnerbooking.com"
DINNERBOOKING_EMAIL = "karl@creative-space.dk"
DINNERBOOKING_PASSWORD = "DBKode2022!"

# Restaurant IDs (same as Azure Function)
RESTAURANT_IDS = {
    '2070': 'Creative Space Frederiksberg',
    '3394': 'Creative Space Lyngby',
    '3395': 'Creative Space Odense',
    '3396': 'Creative Space Østerbro',
    '3398': 'Creative Space Aarhus',
    '3714': 'Creative Space Vejle',
}

# SQL Server (Revenue)
SQL_SERVER = '185.134.253.71,9001'
SQL_DATABASE = 'CreativeSpaceSales'
SQL_USER = 'Creativespace'
SQL_PASSWORD = 'C3RigHl93kFmEy'

# Snowflake
SF_USER = 'AzureFunction'
SF_PASSWORD = 'D*zmkE?k6%1,L42:Asqs'
SF_ACCOUNT = 'qp38588.west-europe.azure'
SF_WAREHOUSE = 'Azure_WH'
SF_ROLE = 'SYSADMIN'


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_dinnerbooking_auth():
    """Get HTTP Basic Auth for DinnerBooking API."""
    return HTTPBasicAuth(DINNERBOOKING_EMAIL, DINNERBOOKING_PASSWORD)


def get_sql_connection():
    """Connect to SQL Server."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def get_snowflake_connection(database):
    """Connect to Snowflake."""
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=database,
        schema='PYTHON_IMPORT',
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
    )


# =============================================================================
# ENDPOINT 1: REVENUE BY DEPARTMENT (SQL Server) - existing
# =============================================================================

@app.route('/api/revenue/by-department', methods=['GET'])
def revenue_by_department():
    """Get revenue by department from SQL Server."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        query = """
            SELECT 
                Department,
                SUM(Amount) as total_revenue
            FROM dbo.SQL_PlecTo
            WHERE CAST(Date AS DATE) >= ?
              AND CAST(Date AS DATE) <= ?
              AND SalesType = 'PosSale'
              AND (ItemGroupText NOT LIKE '%Gavekort%' 
                   AND ItemGroupText NOT LIKE '%GiftUp%'
                   AND ItemGroupText NOT LIKE '%Reklamationer%')
            GROUP BY Department
            ORDER BY Department
        """
        cursor.execute(query, start_date, end_date)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                'department': row[0],
                'total_revenue': float(row[1]) if row[1] else 0
            })

        conn.close()
        return jsonify({'data': results, 'start_date': start_date, 'end_date': end_date})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# ENDPOINT 2: LIVE PAX BY DEPARTMENT (DinnerBooking API - REAL-TIME!)
# =============================================================================

@app.route('/api/pax/live', methods=['GET'])
def pax_live():
    """
    Get LIVE PAX data directly from DinnerBooking API.
    This bypasses Snowflake entirely and gives real-time booking counts.
    
    Query params:
        date: YYYY-MM-DD (default: today)
    """
    copenhagen = pytz.timezone('Europe/Copenhagen')
    date_str = request.args.get('date', datetime.now(copenhagen).strftime('%Y-%m-%d'))

    auth = get_dinnerbooking_auth()
    results = []
    total_pax = 0

    for restaurant_id, restaurant_name in RESTAURANT_IDS.items():
        try:
            url = f"{DINNERBOOKING_BASE_URL}/dk/da-DK/bookings/get_activity_for_day/{restaurant_id}/{date_str}.json"
            r = requests.get(url, auth=auth, timeout=15)
            r.raise_for_status()
            data = r.json()

            bookings = data.get('bookings', [])

            # Sum PAX only for current (non-deleted) bookings
            department_pax = 0
            booking_count = 0
            for booking in bookings:
                status = booking.get('RestaurantBooking', {}).get('b_status', '')
                if status == 'current':
                    pax = booking.get('RestaurantBooking', {}).get('b_pax', 0)
                    try:
                        pax = int(pax)
                    except (ValueError, TypeError):
                        pax = 0
                    department_pax += pax
                    booking_count += 1

            total_pax += department_pax
            results.append({
                'department': restaurant_name,
                'restaurant_id': restaurant_id,
                'pax': department_pax,
                'bookings': booking_count,
            })

            # Respect rate limit: 1 request per second
            time.sleep(1)

        except Exception as e:
            results.append({
                'department': restaurant_name,
                'restaurant_id': restaurant_id,
                'pax': 0,
                'bookings': 0,
                'error': str(e),
            })

    return jsonify({
        'data': results,
        'total_pax': total_pax,
        'date': date_str,
        'source': 'DinnerBooking API (live)',
        'fetched_at': datetime.now(copenhagen).strftime('%Y-%m-%d %H:%M:%S'),
    })


# =============================================================================
# ENDPOINT 3: PAX BY DEPARTMENT (Snowflake - historical/fallback)
# =============================================================================

@app.route('/api/pax/by-department', methods=['GET'])
def pax_by_department():
    """Get PAX by department from Snowflake (historical data, updated nightly)."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    try:
        conn = get_snowflake_connection('DINNERBOOKING')
        cursor = conn.cursor()

        query = """
            SELECT 
                r.NAME as department,
                SUM(b.RESTAURANTBOOKING_B_PAX) as total_pax
            FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
            JOIN DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS r 
                ON b.RESTAURANT_ID = r.ID
            WHERE CAST(b.RESTAURANTBOOKING_B_DATE_TIME AS DATE) >= %s
              AND CAST(b.RESTAURANTBOOKING_B_DATE_TIME AS DATE) <= %s
              AND b.RESTAURANTBOOKING_B_STATUS = 'current'
            GROUP BY r.NAME
            ORDER BY r.NAME
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                'department': row[0],
                'total_pax': int(row[1]) if row[1] else 0
            })

        conn.close()
        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
            'source': 'Snowflake (nightly update)',
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# ENDPOINT 4: LABOR BY DEPARTMENT (Snowflake)
# =============================================================================

@app.route('/api/labor/by-department', methods=['GET'])
def labor_by_department():
    """Get labor cost by department from Snowflake."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        query = """
            SELECT 
                r.NAME as department,
                SUM(p.WAGE) as total_labor
            FROM PLANDAY.PYTHON_IMPORT.PAYROLL p
            JOIN DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS r 
                ON p.DEPARTMENTID = r.ID
            WHERE CAST(p.DATE AS DATE) >= %s
              AND CAST(p.DATE AS DATE) <= %s
            GROUP BY r.NAME
            ORDER BY r.NAME
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                'department': row[0],
                'total_labor': float(row[1]) if row[1] else 0
            })

        conn.close()
        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# ENDPOINT 5: PAX DATE RANGE (DinnerBooking API live - for multiple days)
# =============================================================================

@app.route('/api/pax/live/range', methods=['GET'])
def pax_live_range():
    """
    Get LIVE PAX data for a date range from DinnerBooking API.
    NOTE: Due to rate limits (10 req/min), max range is ~1-2 days efficiently.
    For longer ranges, use the Snowflake endpoint.
    
    Query params:
        start_date: YYYY-MM-DD (default: today)
        end_date: YYYY-MM-DD (default: today)
    """
    copenhagen = pytz.timezone('Europe/Copenhagen')
    start_date_str = request.args.get('start_date', datetime.now(copenhagen).strftime('%Y-%m-%d'))
    end_date_str = request.args.get('end_date', start_date_str)

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Limit range to 7 days to avoid hitting rate limits
    if (end_date - start_date).days > 7:
        return jsonify({
            'error': 'Max date range is 7 days for live API. Use /api/pax/by-department for longer ranges.',
        }), 400

    auth = get_dinnerbooking_auth()
    all_results = []
    grand_total = 0
    requests_made = 0
    rate_limit_start = datetime.now()

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        for restaurant_id, restaurant_name in RESTAURANT_IDS.items():
            try:
                # Rate limiting: 10 requests per minute
                requests_made += 1
                if requests_made >= 10:
                    elapsed = (datetime.now() - rate_limit_start).total_seconds()
                    if elapsed < 60:
                        time.sleep(60 - elapsed)
                    requests_made = 0
                    rate_limit_start = datetime.now()

                url = f"{DINNERBOOKING_BASE_URL}/dk/da-DK/bookings/get_activity_for_day/{restaurant_id}/{date_str}.json"
                r = requests.get(url, auth=auth, timeout=15)
                r.raise_for_status()
                data = r.json()

                bookings = data.get('bookings', [])
                department_pax = 0
                booking_count = 0
                for booking in bookings:
                    status = booking.get('RestaurantBooking', {}).get('b_status', '')
                    if status == 'current':
                        pax = booking.get('RestaurantBooking', {}).get('b_pax', 0)
                        try:
                            pax = int(pax)
                        except (ValueError, TypeError):
                            pax = 0
                        department_pax += pax
                        booking_count += 1

                grand_total += department_pax
                all_results.append({
                    'date': date_str,
                    'department': restaurant_name,
                    'restaurant_id': restaurant_id,
                    'pax': department_pax,
                    'bookings': booking_count,
                })

                time.sleep(1)

            except Exception as e:
                all_results.append({
                    'date': date_str,
                    'department': restaurant_name,
                    'restaurant_id': restaurant_id,
                    'pax': 0,
                    'bookings': 0,
                    'error': str(e),
                })

        current_date += timedelta(days=1)

    return jsonify({
        'data': all_results,
        'total_pax': grand_total,
        'start_date': start_date_str,
        'end_date': end_date_str,
        'source': 'DinnerBooking API (live)',
        'fetched_at': datetime.now(copenhagen).strftime('%Y-%m-%d %H:%M:%S'),
    })


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'endpoints': [
            'GET /api/revenue/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD',
            'GET /api/pax/live?date=YYYY-MM-DD  (REAL-TIME from DinnerBooking API)',
            'GET /api/pax/live/range?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD  (live, max 7 days)',
            'GET /api/pax/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD  (Snowflake)',
            'GET /api/labor/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD',
        ]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
