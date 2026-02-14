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
        ORDER BY department
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
    """
    Get labor cost by department from Snowflake (Planday PAYROLL).
    Optionally includes benchmark period for comparison.
    
    Query params:
        start_date, end_date: Current period (YYYY-MM-DD)
        benchmark_start, benchmark_end: Benchmark period (optional, YYYY-MM-DD)
    """
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    benchmark_start = request.args.get('benchmark_start', '')
    benchmark_end = request.args.get('benchmark_end', '')

    # Map Planday department IDs to restaurant names
    base_query = """
        SELECT 
            CASE p.DEPARTMENTID
                WHEN 162164 THEN 'Creative Space Frederiksberg'
                WHEN 164684 THEN 'Creative Space Lyngby'
                WHEN 162209 THEN 'Creative Space Odense'
                WHEN 162162 THEN 'Creative Space Østerbro'
                WHEN 162208 THEN 'Creative Space Aarhus'
                WHEN 164717 THEN 'Creative Space Vejle'
                ELSE 'Unknown'
            END as department,
            SUM(p.SALARY) as total_labor
        FROM PLANDAY.PYTHON_IMPORT.PAYROLL p
        WHERE CAST(p.DATE AS DATE) >= %s
          AND CAST(p.DATE AS DATE) <= %s
          AND p.DEPARTMENTID IN (162164, 164684, 162209, 162162, 162208, 164717)
        GROUP BY p.DEPARTMENTID
        ORDER BY department
    """

    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        # Current period
        cursor.execute(base_query, (start_date, end_date))
        current_rows = cursor.fetchall()
        current_map = {}
        for row in current_rows:
            current_map[row[0]] = float(row[1]) if row[1] else 0

        # Benchmark period (if provided)
        benchmark_map = {}
        if benchmark_start and benchmark_end:
            cursor.execute(base_query, (benchmark_start, benchmark_end))
            benchmark_rows = cursor.fetchall()
            for row in benchmark_rows:
                benchmark_map[row[0]] = float(row[1]) if row[1] else 0

        # Combine results
        all_departments = sorted(set(list(current_map.keys()) + list(benchmark_map.keys())))
        results = []
        for dept in all_departments:
            current_labor = current_map.get(dept, 0)
            benchmark_labor = benchmark_map.get(dept, 0)

            entry = {
                'department': dept,
                'current_labor': round(current_labor, 2),
            }
            if benchmark_start and benchmark_end:
                entry['benchmark_labor'] = round(benchmark_labor, 2)
                entry['change'] = round(current_labor - benchmark_labor, 2)

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
# ENDPOINT 5: DAILY LABOR VS REVENUE (Snowflake + SQL Server)
# =============================================================================

@app.route('/api/labor-vs-revenue/daily', methods=['GET'])
def labor_vs_revenue_daily():
    """
    Get daily labor vs revenue per department.
    
    Query params:
        start_date, end_date: Period (YYYY-MM-DD)
        department: Optional filter (e.g. 'Creative Space Frederiksberg', or 'all')
    """
    start_date = request.args.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    department_filter = request.args.get('department', 'all')

    labor_query = """
        SELECT 
            CAST(p.DATE AS DATE) as day,
            CASE p.DEPARTMENTID
                WHEN 162164 THEN 'Creative Space Frederiksberg'
                WHEN 164684 THEN 'Creative Space Lyngby'
                WHEN 162209 THEN 'Creative Space Odense'
                WHEN 162162 THEN 'Creative Space Østerbro'
                WHEN 162208 THEN 'Creative Space Aarhus'
                WHEN 164717 THEN 'Creative Space Vejle'
                ELSE 'Unknown'
            END as department,
            SUM(p.SALARY) as labor
        FROM PLANDAY.PYTHON_IMPORT.PAYROLL p
        WHERE CAST(p.DATE AS DATE) >= %s
          AND CAST(p.DATE AS DATE) <= %s
          AND p.DEPARTMENTID IN (162164, 164684, 162209, 162162, 162208, 164717)
        GROUP BY CAST(p.DATE AS DATE), p.DEPARTMENTID
        ORDER BY day, department
    """

    revenue_query = """
        SELECT 
            CAST(Date AS DATE) as day,
            Department as department,
            SUM(TotalExclVAT) as revenue
        FROM dbo.SQL_PlecTo
        WHERE CAST(Date AS DATE) >= ?
          AND CAST(Date AS DATE) <= ?
          AND SalesType = 'PosSale'
          AND (ItemGroupText NOT LIKE '%Gavekort%' 
               AND ItemGroupText NOT LIKE '%GiftUp%'
               AND ItemGroupText NOT LIKE '%Reklamationer%')
        GROUP BY CAST(Date AS DATE), Department
        ORDER BY day, department
    """

    sf_conn = None
    sql_conn = None
    try:
        # Get labor from Snowflake
        sf_conn = get_snowflake_connection('PLANDAY')
        cursor = sf_conn.cursor()
        cursor.execute(labor_query, (start_date, end_date))
        labor_rows = cursor.fetchall()

        # labor_map: {date: {department: labor}}
        labor_map = {}
        for row in labor_rows:
            day = str(row[0])
            dept = row[1]
            labor = float(row[2]) if row[2] else 0
            if day not in labor_map:
                labor_map[day] = {}
            labor_map[day][dept] = labor

        # Get revenue from SQL Server
        sql_conn = get_sql_connection()
        sql_cursor = sql_conn.cursor()
        sql_cursor.execute(revenue_query, start_date, end_date)
        revenue_rows = sql_cursor.fetchall()

        # revenue_map: {date: {department: revenue}}
        # Normalize revenue department names to match labor names
        rev_name_map = {
            'Frederiksberg': 'Creative Space Frederiksberg',
            'Lyngby': 'Creative Space Lyngby',
            'Odense': 'Creative Space Odense',
            'Østerbro': 'Creative Space Østerbro',
            'Aarhus': 'Creative Space Aarhus',
            'Vejle': 'Creative Space Vejle',
        }
        raw_dept_names = set()
        revenue_map = {}
        for row in revenue_rows:
            day = str(row[0])
            raw_dept = str(row[1]).strip()
            raw_dept_names.add(raw_dept)
            dept = rev_name_map.get(raw_dept, raw_dept)
            revenue = float(row[2]) if row[2] else 0
            if day not in revenue_map:
                revenue_map[day] = {}
            if dept in revenue_map[day]:
                revenue_map[day][dept] += revenue
            else:
                revenue_map[day][dept] = revenue

        # Combine
        all_days = sorted(set(list(labor_map.keys()) + list(revenue_map.keys())))
        all_departments = sorted(set(
            [d for day_map in labor_map.values() for d in day_map.keys()] +
            [d for day_map in revenue_map.values() for d in day_map.keys()]
        ))

        results = []
        for day in all_days:
            day_labor = labor_map.get(day, {})
            day_revenue = revenue_map.get(day, {})

            for dept in all_departments:
                if department_filter != 'all' and dept != department_filter:
                    continue
                labor = day_labor.get(dept, 0)
                revenue = day_revenue.get(dept, 0)
                pct = (labor / revenue * 100) if revenue > 0 else 0

                results.append({
                    'date': day,
                    'department': dept,
                    'revenue': round(revenue, 2),
                    'labor': round(labor, 2),
                    'labor_pct': round(pct, 1),
                })

        return jsonify({
            'data': results,
            'start_date': start_date,
            'end_date': end_date,
            'department_filter': department_filter,
            'departments': all_departments,
            'raw_revenue_departments': sorted(list(raw_dept_names)),
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if sf_conn:
            sf_conn.close()
        if sql_conn:
            sql_conn.close()

@app.route('/api/debug/absence-check', methods=['GET'])
def debug_absence_check():
    """Debug: Check Absence table structure and sample data."""
    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM PLANDAY.PYTHON_IMPORT.ABSENCE LIMIT 5")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        samples = [dict(zip(columns, [str(v) for v in row])) for row in rows]

        cursor.execute("SELECT COUNT(*) FROM PLANDAY.PYTHON_IMPORT.ABSENCE")
        total = cursor.fetchone()[0]

        return jsonify({
            'columns': columns,
            'total_rows': total,
            'samples': samples,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


@app.route('/api/debug/employees-check', methods=['GET'])
def debug_employees_check():
    """Debug: Check Employees table structure and sample data."""
    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM PLANDAY.PYTHON_IMPORT.EMPLOYEES LIMIT 3")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        samples = [dict(zip(columns, [str(v) for v in row])) for row in rows]

        cursor.execute("SELECT COUNT(*) FROM PLANDAY.PYTHON_IMPORT.EMPLOYEES")
        total = cursor.fetchone()[0]

        return jsonify({
            'columns': columns,
            'total_rows': total,
            'samples': samples,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


@app.route('/api/debug/absence-accounts', methods=['GET'])
def debug_absence_accounts():
    """Debug: Check distinct absence account IDs to identify sickness vs holiday."""
    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        cursor.execute("""
            SELECT REGISTRATIONS_ACCOUNT_ID, COUNT(*) as cnt
            FROM PLANDAY.PYTHON_IMPORT.ABSENCE
            GROUP BY REGISTRATIONS_ACCOUNT_ID
            ORDER BY cnt DESC
        """)
        rows = cursor.fetchall()
        accounts = [{'account_id': str(r[0]), 'count': r[1]} for r in rows]

        return jsonify({'absence_accounts': accounts})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


@app.route('/api/debug/shift-types', methods=['GET'])
def debug_shift_types():
    """Debug: Fetch shift types from Planday API to map IDs to names."""
    try:
        # Get Planday token
        token_url = "https://id.planday.com/connect/token"
        token_payload = 'client_id=9ff9461c-a729-4e49-8ae8-d231e2123263&grant_type=refresh_token&refresh_token=uAURbS3-hUKWtpuRbmTWBA'
        token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token_res = http_requests.post(token_url, headers=token_headers, data=token_payload)
        access_token = token_res.json()['access_token']

        # Get shift types
        url = "https://openapi.planday.com/scheduling/v1/shifttypes"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'X-ClientId': '9ff9461c-a729-4e49-8ae8-d231e2123263'
        }
        res = http_requests.get(url, headers=headers)
        data = res.json()

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/employee-groups', methods=['GET'])
def debug_employee_groups():
    """Debug: Fetch employee groups from Planday API."""
    try:
        token_url = "https://id.planday.com/connect/token"
        token_payload = 'client_id=9ff9461c-a729-4e49-8ae8-d231e2123263&grant_type=refresh_token&refresh_token=uAURbS3-hUKWtpuRbmTWBA'
        token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token_res = http_requests.post(token_url, headers=token_headers, data=token_payload)
        access_token = token_res.json()['access_token']

        url = "https://openapi.planday.com/hr/v1.0/employeegroups"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'X-ClientId': '9ff9461c-a729-4e49-8ae8-d231e2123263'
        }
        res = http_requests.get(url, headers=headers)
        data = res.json()

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/shifts-check', methods=['GET'])
def debug_shifts_check():
    """Debug: Check Shifts table structure, statuses and sample sick shifts."""
    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        # Get columns
        cursor.execute("SELECT * FROM PLANDAY.PYTHON_IMPORT.SHIFTS LIMIT 1")
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        sample = dict(zip(columns, [str(v) for v in row])) if row else {}

        # Get distinct statuses
        cursor.execute("""
            SELECT STATUS, COUNT(*) as cnt
            FROM PLANDAY.PYTHON_IMPORT.SHIFTS
            GROUP BY STATUS
            ORDER BY cnt DESC
        """)
        statuses = [{'status': str(r[0]), 'count': r[1]} for r in cursor.fetchall()]

        # Get distinct shift type IDs
        cursor.execute("""
            SELECT SHIFTTYPEID, COUNT(*) as cnt
            FROM PLANDAY.PYTHON_IMPORT.SHIFTS
            GROUP BY SHIFTTYPEID
            ORDER BY cnt DESC
        """)
        shift_types = [{'shift_type_id': str(r[0]), 'count': r[1]} for r in cursor.fetchall()]

        # Get distinct employee group IDs
        cursor.execute("""
            SELECT EMPLOYEEGROUPID, COUNT(*) as cnt
            FROM PLANDAY.PYTHON_IMPORT.SHIFTS
            GROUP BY EMPLOYEEGROUPID
            ORDER BY cnt DESC
        """)
        emp_groups = [{'employee_group_id': str(r[0]), 'count': r[1]} for r in cursor.fetchall()]

        # Get sample shifts with non-null SHIFTTYPEID
        cursor.execute("""
            SELECT * FROM PLANDAY.PYTHON_IMPORT.SHIFTS
            WHERE SHIFTTYPEID IS NOT NULL
            LIMIT 5
        """)
        typed_rows = cursor.fetchall()
        typed_samples = [dict(zip(columns, [str(v) for v in row])) for row in typed_rows]

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM PLANDAY.PYTHON_IMPORT.SHIFTS")
        total = cursor.fetchone()[0]

        return jsonify({
            'columns': columns,
            'total_rows': total,
            'sample': sample,
            'statuses': statuses,
            'shift_type_ids': shift_types,
            'employee_group_ids': emp_groups,
            'samples_with_shift_type': typed_samples,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


@app.route('/api/debug/payroll-check', methods=['GET'])
def debug_payroll_check():
    """Debug: Check Payroll table structure and sample data."""
    conn = None
    try:
        conn = get_snowflake_connection('PLANDAY')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM PLANDAY.PYTHON_IMPORT.PAYROLL LIMIT 1")
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        sample = dict(zip(columns, [str(v) for v in row])) if row else {}

        cursor.execute("""
            SELECT DEPARTMENTID, COUNT(*), SUM(SALARY)
            FROM PLANDAY.PYTHON_IMPORT.PAYROLL
            WHERE CAST(DATE AS DATE) >= '2025-02-08'
              AND CAST(DATE AS DATE) <= '2025-02-16'
            GROUP BY DEPARTMENTID
            ORDER BY DEPARTMENTID
        """)
        dept_rows = cursor.fetchall()

        return jsonify({
            'columns': columns,
            'sample_row': sample,
            'by_department': [{'dept_id': str(r[0]), 'count': r[1], 'sum_salary': float(r[2]) if r[2] else 0} for r in dept_rows],
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()


# =============================================================================
# DASHBOARD AUTH
# =============================================================================

from functools import wraps

# Username -> department filter mapping
# 'admin' sees all, location usernames see only their data
DASHBOARD_USERS = {
    'admin': 'all',
    'frederiksberg': 'Creative Space Frederiksberg',
    'lyngby': 'Creative Space Lyngby',
    'odense': 'Creative Space Odense',
    'østerbro': 'Creative Space Østerbro',
    'osterbro': 'Creative Space Østerbro',
    'aarhus': 'Creative Space Aarhus',
    'vejle': 'Creative Space Vejle',
}

DASHBOARD_PASSWORD = 'CS2026!'

def check_dashboard_auth(username, password):
    return username.lower() in DASHBOARD_USERS and password == DASHBOARD_PASSWORD

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_dashboard_auth(auth.username, auth.password):
            return ('Unauthorized', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
        return f(*args, **kwargs)
    return decorated

def get_user_department():
    """Get the department filter for the current user."""
    auth = request.authorization
    if auth:
        return DASHBOARD_USERS.get(auth.username.lower(), 'all')
    return 'all'


@app.route('/dashboard')
@requires_auth
def dashboard():
    """Serve the dashboard HTML page."""
    return send_from_directory('static', 'dashboard.html')


@app.route('/api/dashboard/user-info')
@requires_auth
def dashboard_user_info():
    """Return the current user's department filter."""
    dept = get_user_department()
    return jsonify({
        'username': request.authorization.username,
        'department_filter': dept,
        'is_admin': dept == 'all',
    })


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
