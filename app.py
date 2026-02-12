from flask import Flask, jsonify, request
from flask_cors import CORS
import requests as http_requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import pytz
import time
import io
import json
from zipfile import ZipFile
import os

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
    import pyodbc
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
    import snowflake.connector
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=database,
        schema='PYTHON_IMPORT',
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
    )


def get_dump_for_restaurant(auth, restaurant_id):
    """
    Download and parse the full booking dump for a single restaurant.
    Returns a list of all booking dicts.
    """
    url = f"{DINNERBOOKING_BASE_URL}/dk/da-DK/bookings/dump/{restaurant_id}.zip"
    r = http_requests.get(url, auth=auth, timeout=120)
    r.raise_for_status()

    all_bookings = []
    zip_file = io.BytesIO(r.content)
    with ZipFile(zip_file, 'r') as archive:
        for file_name in archive.namelist():
            file_content = archive.read(file_name)
            bookings = json.loads(file_content.decode('utf-8'))
            all_bookings.extend(bookings)

    return all_bookings


def filter_bookings_by_date(bookings, date_str, status='current'):
    """
    Filter bookings for a specific date and status.
    Returns total PAX and booking count.
    """
    total_pax = 0
    booking_count = 0

    for booking in bookings:
        rb = booking.get('RestaurantBooking', {})
        booking_date = rb.get('b_date_time', '')
        booking_status = rb.get('b_status', '')

        if booking_date and booking_date[:10] == date_str and booking_status == status:
            pax = rb.get('b_pax', 0)
            try:
                pax = int(pax)
            except (ValueError, TypeError):
                pax = 0
            total_pax += pax
            booking_count += 1

    return total_pax, booking_count


# =============================================================================
# ENDPOINT 1: REVENUE BY DEPARTMENT (SQL Server)
# =============================================================================

@app.route('/api/revenue/by-department', methods=['GET'])
def revenue_by_department():
    """Get revenue by department from SQL Server."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    conn = None
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

        return jsonify({'data': results, 'start_date': start_date, 'end_date': end_date})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


# =============================================================================
# ENDPOINT 2: LIVE PAX BY DEPARTMENT (DinnerBooking API - REAL-TIME)
# =============================================================================

@app.route('/api/pax/live', methods=['GET'])
def pax_live():
    """
    Get LIVE PAX data directly from DinnerBooking API.
    Uses the dump endpoint (full booking export per restaurant) to get ALL bookings,
    then filters by date and status='current'.
    
    This matches exactly what DinnerBooking shows in their UI.
    
    Note: Downloads ~170K bookings per restaurant as zip. 
    Total response time: ~30-60 seconds for all 6 restaurants.
    
    Query params:
        date: YYYY-MM-DD (default: today in Copenhagen timezone)
    """
    copenhagen = pytz.timezone('Europe/Copenhagen')
    date_str = request.args.get('date', datetime.now(copenhagen).strftime('%Y-%m-%d'))

    auth = get_dinnerbooking_auth()
    results = []
    total_pax = 0

    for restaurant_id, restaurant_name in RESTAURANT_IDS.items():
        try:
            bookings = get_dump_for_restaurant(auth, restaurant_id)
            department_pax, booking_count = filter_bookings_by_date(bookings, date_str)

            total_pax += department_pax
            results.append({
                'department': restaurant_name,
                'restaurant_id': restaurant_id,
                'pax': department_pax,
                'bookings': booking_count,
            })

            # Respect rate limit: 1 request per second between restaurants
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
    """Get PAX by department from Snowflake (historical data, updated hourly)."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    conn = None
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

        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
            'source': 'Snowflake (hourly update)',
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


# =============================================================================
# ENDPOINT 4: LABOR BY DEPARTMENT (Snowflake)
# =============================================================================

@app.route('/api/labor/by-department', methods=['GET'])
def labor_by_department():
    """Get labor cost by department from Snowflake."""
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    conn = None
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

        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/api/debug/dump-sample', methods=['GET'])
def debug_dump_sample():
    """
    DEBUG: Returns a sample of raw dump data to verify JSON structure.
    Shows first 3 bookings from Frederiksberg (2070).
    Remove this endpoint after verification.
    """
    auth = get_dinnerbooking_auth()
    restaurant_id = request.args.get('restaurant_id', '2070')
    date_str = request.args.get('date', '')
    
    try:
        url = f"{DINNERBOOKING_BASE_URL}/dk/da-DK/bookings/dump/{restaurant_id}.zip"
        r = http_requests.get(url, auth=auth, timeout=120)
        r.raise_for_status()

        zip_file = io.BytesIO(r.content)
        file_info = []
        sample_bookings = []
        total_bookings = 0
        matching_bookings = 0

        with ZipFile(zip_file, 'r') as archive:
            for file_name in archive.namelist():
                file_content = archive.read(file_name)
                bookings = json.loads(file_content.decode('utf-8'))
                file_info.append({
                    'file_name': file_name,
                    'booking_count': len(bookings),
                })
                total_bookings += len(bookings)

                for booking in bookings:
                    rb = booking.get('RestaurantBooking', {})
                    bd = rb.get('b_date_time', '')
                    
                    if date_str and bd and bd[:10] == date_str:
                        matching_bookings += 1
                        if len(sample_bookings) < 3:
                            sample_bookings.append(booking)
                    elif not date_str and len(sample_bookings) < 3:
                        sample_bookings.append(booking)

        return jsonify({
            'restaurant_id': restaurant_id,
            'zip_files': file_info,
            'total_bookings_in_dump': total_bookings,
            'matching_bookings_for_date': matching_bookings if date_str else 'no date filter',
            'date_filter': date_str or 'none',
            'sample_bookings': sample_bookings,
            'keys_in_first_booking': list(sample_bookings[0].keys()) if sample_bookings else [],
            'restaurant_booking_keys': list(sample_bookings[0].get('RestaurantBooking', {}).keys()) if sample_bookings else [],
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'endpoints': [
            'GET /api/revenue/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD',
            'GET /api/pax/live?date=YYYY-MM-DD  (REAL-TIME from DinnerBooking API, ~30-60s)',
            'GET /api/pax/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD  (Snowflake, fast)',
            'GET /api/labor/by-department?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD',
        ]
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
