from flask import Flask, jsonify, request, send_from_directory
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

app = Flask(__name__, static_folder='static')
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
    """
    Get revenue by department from SQL Server.
    Optionally includes benchmark period for comparison.
    
    Query params:
        start_date, end_date: Current period (YYYY-MM-DD)
        benchmark_start, benchmark_end: Benchmark period (optional, YYYY-MM-DD)
    """
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    benchmark_start = request.args.get('benchmark_start', '')
    benchmark_end = request.args.get('benchmark_end', '')

    base_query = """
        SELECT 
            Department,
            SUM(TotalExclVAT) as total_revenue
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

    conn = None
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Current period
        cursor.execute(base_query, start_date, end_date)
        current_rows = cursor.fetchall()
        current_map = {}
        for row in current_rows:
            current_map[row[0]] = float(row[1]) if row[1] else 0

        # Benchmark period (if provided)
        benchmark_map = {}
        if benchmark_start and benchmark_end:
            cursor.execute(base_query, benchmark_start, benchmark_end)
            benchmark_rows = cursor.fetchall()
            for row in benchmark_rows:
                benchmark_map[row[0]] = float(row[1]) if row[1] else 0

        # Combine results
        all_departments = sorted(set(list(current_map.keys()) + list(benchmark_map.keys())))
        results = []
        for dept in all_departments:
            current_rev = current_map.get(dept, 0)
            benchmark_rev = benchmark_map.get(dept, 0)
            change_percent = ((current_rev - benchmark_rev) / benchmark_rev * 100) if benchmark_rev else 0

            entry = {
                'department': dept,
                'current_revenue': round(current_rev, 2),
            }
            if benchmark_start and benchmark_end:
                entry['benchmark_revenue'] = round(benchmark_rev, 2)
                entry['change_percent'] = round(change_percent, 2)

            results.append(entry)

        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
            'benchmark_start': benchmark_start or None,
            'benchmark_end': benchmark_end or None,
        })

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
    Uses the 'Bookings for a day' endpoint with status filter for 'current' only.
    This excludes waiting, deleted, no_show, payment_error, and moved bookings.
    
    Query params:
        date: YYYY-MM-DD (default: today in Copenhagen timezone)
    """
    copenhagen = pytz.timezone('Europe/Copenhagen')
    date_str = request.args.get('date', datetime.now(copenhagen).strftime('%Y-%m-%d'))
    target_date = datetime.strptime(date_str, '%Y-%m-%d')

    auth = get_dinnerbooking_auth()
    results = []
    total_pax = 0

    for restaurant_id, restaurant_name in RESTAURANT_IDS.items():
        try:
            department_pax = 0
            booking_count = 0
            page = 1
            has_more = True

            while has_more:
                url = (
                    f"{DINNERBOOKING_BASE_URL}/dk/da-DK/bookings/restaurant/"
                    f"{restaurant_id}/{target_date.year}/{target_date.month}/{target_date.day}.json"
                    f"?page={page}"
                    f"&filterFormId=RestaurantBooking"
                    f"&RestaurantBooking[b_status]=current"
                )
                r = http_requests.get(url, auth=auth, timeout=30)
                r.raise_for_status()
                data = r.json()

                bookings = data.get('restaurantBookings', [])

                for booking in bookings:
                    rb = booking.get('RestaurantBooking', {})
                    pax = rb.get('b_pax', 0)
                    try:
                        pax = int(pax)
                    except (ValueError, TypeError):
                        pax = 0
                    department_pax += pax
                    booking_count += 1

                # Check pagination
                paging = data.get('paging', {}).get('RestaurantBooking', {})
                page_count = paging.get('pageCount', 1)
                has_more = page < page_count
                page += 1

                time.sleep(1)  # Rate limit: 1 request per second

            total_pax += department_pax
            results.append({
                'department': restaurant_name,
                'restaurant_id': restaurant_id,
                'pax': department_pax,
                'bookings': booking_count,
            })

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
    """
    Get PAX by department from Snowflake (historical data, updated hourly).
    Optionally includes benchmark period for comparison.
    
    Query params:
        start_date, end_date: Current period (YYYY-MM-DD)
        benchmark_start, benchmark_end: Benchmark period (optional, YYYY-MM-DD)
    """
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    benchmark_start = request.args.get('benchmark_start', '')
    benchmark_end = request.args.get('benchmark_end', '')

    base_query = """
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

    conn = None
    try:
        conn = get_snowflake_connection('DINNERBOOKING')
        cursor = conn.cursor()

        # Current period
        cursor.execute(base_query, (start_date, end_date))
        current_rows = cursor.fetchall()
        current_map = {}
        for row in current_rows:
            current_map[row[0]] = int(row[1]) if row[1] else 0

        # Benchmark period (if provided)
        benchmark_map = {}
        if benchmark_start and benchmark_end:
            cursor.execute(base_query, (benchmark_start, benchmark_end))
            benchmark_rows = cursor.fetchall()
            for row in benchmark_rows:
                benchmark_map[row[0]] = int(row[1]) if row[1] else 0

        # Combine results
        all_departments = sorted(set(list(current_map.keys()) + list(benchmark_map.keys())))
        results = []
        for dept in all_departments:
            current_pax = current_map.get(dept, 0)
            benchmark_pax = benchmark_map.get(dept, 0)
            change = current_pax - benchmark_pax

            entry = {
                'department': dept,
                'current_pax': current_pax,
            }
            if benchmark_start and benchmark_end:
                entry['benchmark_pax'] = benchmark_pax
                entry['change'] = change

            results.append(entry)

        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
            'benchmark_start': benchmark_start or None,
            'benchmark_end': benchmark_end or None,
            'source': 'Snowflake (hourly update)',
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


# =============================================================================
# DEBUG: Check Snowflake bookings
# =============================================================================

@app.route('/api/debug/snowflake-check', methods=['GET'])
def debug_snowflake_check():
    """Debug: Check Snowflake bookings table status."""
    conn = None
    try:
        conn = get_snowflake_connection('DINNERBOOKING')
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS")
        total = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*), MIN(RESTAURANTBOOKING_B_DATE_TIME), MAX(RESTAURANTBOOKING_B_DATE_TIME)
            FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS
            WHERE RESTAURANTBOOKING_B_STATUS = 'current'
        """)
        row = cursor.fetchone()

        cursor.execute("""
            SELECT RESTAURANT_ID, COUNT(*), SUM(RESTAURANTBOOKING_B_PAX)
            FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS
            WHERE CAST(RESTAURANTBOOKING_B_DATE_TIME AS DATE) = CURRENT_DATE()
              AND RESTAURANTBOOKING_B_STATUS = 'current'
            GROUP BY RESTAURANT_ID
        """)
        today_rows = cursor.fetchall()

        cursor.execute("SELECT * FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS LIMIT 1")
        columns = [desc[0] for desc in cursor.description]

        return jsonify({
            'total_rows': total,
            'current_status_count': row[0],
            'min_date': str(row[1]),
            'max_date': str(row[2]),
            'today_by_restaurant': [{'restaurant_id': r[0], 'count': r[1], 'pax': int(r[2])} for r in today_rows],
            'columns': columns,
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
            CASE b.RESTAURANT_ID
                WHEN '2070' THEN 'Creative Space Frederiksberg'
                WHEN '3394' THEN 'Creative Space Lyngby'
                WHEN '3395' THEN 'Creative Space Odense'
                WHEN '3396' THEN 'Creative Space Østerbro'
                WHEN '3398' THEN 'Creative Space Aarhus'
                WHEN '3714' THEN 'Creative Space Vejle'
                ELSE 'Unknown (' || b.RESTAURANT_ID || ')'
            END as department,
            SUM(b.RESTAURANTBOOKING_B_PAX) as total_pax
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        WHERE CAST(b.RESTAURANTBOOKING_B_DATE_TIME AS DATE) >= %s
          AND CAST(b.RESTAURANTBOOKING_B_DATE_TIME AS DATE) <= %s
          AND b.RESTAURANTBOOKING_B_STATUS = 'current'
        GROUP BY b.RESTAURANT_ID
        ORDER BY department        """
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
# DASHBOARD
# =============================================================================

@app.route('/dashboard')
def dashboard():
    """Serve the dashboard HTML page."""
    return send_from_directory('static', 'dashboard.html')


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
