"""
Creative Space Dashboard Backend API
Forbinder til Snowflake og leverer data til Retool dashboard
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import snowflake.connector
import pyodbc
from datetime import datetime, timedelta
import os
import csv
import io
from functools import wraps

app = Flask(__name__)
CORS(app)  # Tillad Retool at kalde API'en

# Snowflake forbindelse
SNOWFLAKE_CONFIG = {
    'user': os.environ.get('SNOWFLAKE_USER', 'Karl_admin'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD', 'CSKode2022'),
    'account': 'qp38588.west-europe.azure',
    'warehouse': 'powerbi_wh',
}

# SQL Server forbindelse (for revenue data)
SQL_SERVER_CONFIG = {
    'server': '185.134.253.71,9001',
    'database': 'CreativeSpaceSales',
    'username': os.environ.get('SQL_SERVER_USER', 'Creativespace'),
    'password': os.environ.get('SQL_SERVER_PASSWORD', 'C3RigHl93kFmEy'),
}

def get_snowflake_connection():
    """Opret Snowflake forbindelse"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def get_sql_server_connection():
    """Opret SQL Server forbindelse"""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER_CONFIG['server']};"
        f"DATABASE={SQL_SERVER_CONFIG['database']};"
        f"UID={SQL_SERVER_CONFIG['username']};"
        f"PWD={SQL_SERVER_CONFIG['password']};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def query_snowflake(query, params=None):
    """Udfør query og returner som liste af dicts"""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return results
    finally:
        conn.close()

def query_sql_server(query, params=None):
    """Udfør SQL Server query og returner som liste af dicts"""
    conn = get_sql_server_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return results
    finally:
        conn.close()

# ==================== HEALTH CHECK ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check om API og database forbindelse virker"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        result = cursor.fetchone()
        conn.close()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': str(result[0])
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/pax/by-department', methods=['GET'])
def get_pax_by_department():
    """Get PAX (guests) by department with benchmark comparison"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        benchmark_start = request.args.get('benchmark_start')
        benchmark_end = request.args.get('benchmark_end')
        
        # Query current period PAX from Snowflake
        query_current = """
        SELECT 
            r.NAME as DEPARTMENT,
            SUM(CAST(b.RESTAURANTBOOKING_B_PAX AS NUMBER)) as PAX
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        JOIN AJOUR.PYTHON_IMPORT.RESTAURANTS r ON b.RESTAURANTBOOKING_B_RESTAURANT = r.ID
        WHERE TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= TO_DATE('{}', 'YYYY-MM-DD')
          AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= TO_DATE('{}', 'YYYY-MM-DD')
        GROUP BY r.NAME
        ORDER BY SUM(CAST(b.RESTAURANTBOOKING_B_PAX AS NUMBER)) DESC
        """.format(start_date, end_date)
        
        current = query_snowflake(query_current)
        
        # Query benchmark period PAX
        query_benchmark = """
        SELECT 
            r.NAME as DEPARTMENT,
            SUM(CAST(b.RESTAURANTBOOKING_B_PAX AS NUMBER)) as PAX
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        JOIN AJOUR.PYTHON_IMPORT.RESTAURANTS r ON b.RESTAURANTBOOKING_B_RESTAURANT = r.ID
        WHERE TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= TO_DATE('{}', 'YYYY-MM-DD')
          AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= TO_DATE('{}', 'YYYY-MM-DD')
        GROUP BY r.NAME
        """.format(benchmark_start, benchmark_end)
        
        benchmark = query_snowflake(query_benchmark)
        
        # Combine results
        results = []
        current_dict = {row['DEPARTMENT']: row['PAX'] for row in current}
        benchmark_dict = {row['DEPARTMENT']: row['PAX'] for row in benchmark}
        
        all_departments = set(current_dict.keys()) | set(benchmark_dict.keys())
        
        for dept in all_departments:
            current_pax = current_dict.get(dept, 0)
            benchmark_pax = benchmark_dict.get(dept, 0)
            
            if benchmark_pax > 0:
                change_pct = ((current_pax - benchmark_pax) / benchmark_pax) * 100
            else:
                change_pct = 0
            
            results.append({
                'department': dept,
                'current_pax': current_pax,
                'benchmark_pax': benchmark_pax,
                'change_percent': round(change_pct, 2)
            })
        
        # Sort by current PAX descending
        results.sort(key=lambda x: x['current_pax'], reverse=True)
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/debug/sql-server', methods=['GET'])
def debug_sql_server():
    """Test SQL Server connection with detailed error info"""
    try:
        import socket
        
        # Test 1: Can we resolve the hostname?
        server_parts = SQL_SERVER_CONFIG['server'].split(',')
        host = server_parts[0]
        port = int(server_parts[1]) if len(server_parts) > 1 else 1433
        
        try:
            ip = socket.gethostbyname(host)
            dns_status = f"OK - Resolved to {ip}"
        except Exception as e:
            dns_status = f"FAILED - {str(e)}"
        
        # Test 2: Can we connect to the port?
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            network_status = "OK - Port is reachable" if result == 0 else f"FAILED - Port {port} not reachable"
        except Exception as e:
            network_status = f"FAILED - {str(e)}"
        
        # Test 3: Try SQL Server connection
        try:
            conn = get_sql_server_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            conn.close()
            sql_status = f"OK - Connected! Version: {version[:100]}"
        except Exception as e:
            sql_status = f"FAILED - {str(e)}"
        
        return jsonify({
            'server': SQL_SERVER_CONFIG['server'],
            'database': SQL_SERVER_CONFIG['database'],
            'dns_resolution': dns_status,
            'network_connectivity': network_status,
            'sql_server_connection': sql_status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ==================== PAX (GÆSTER) ====================

@app.route('/api/pax/by-department', methods=['GET'])
@app.route('/api/pax/summary', methods=['GET'])
def get_pax_summary():
    """
    Hent samlet PAX summary med benchmark
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        query_template = """
        SELECT 
            SUM(RESTAURANTBOOKING_B_PERSONS) as TOTAL_PAX,
            COUNT(*) as TOTAL_BOOKINGS
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS
        WHERE DATE(RESTAURANTBOOKING_B_DATE_TIME) >= '{}'
          AND DATE(RESTAURANTBOOKING_B_DATE_TIME) <= '{}'
        """
        
        current = query_snowflake(query_template.format(start_date, end_date))[0]
        benchmark = query_snowflake(query_template.format(benchmark_start, benchmark_end))[0]
        
        current_pax = current['TOTAL_PAX'] or 0
        benchmark_pax = benchmark['TOTAL_PAX'] or 0
        
        change_percent = 0
        if benchmark_pax > 0:
            change_percent = ((current_pax - benchmark_pax) / benchmark_pax) * 100
        
        return jsonify({
            'current_pax': current_pax,
            'benchmark_pax': benchmark_pax,
            'change_percent': round(change_percent, 2),
            'current_bookings': current['TOTAL_BOOKINGS'],
            'benchmark_bookings': benchmark['TOTAL_BOOKINGS']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== BELÆGNINGSGRAD (OCCUPANCY) ====================

@app.route('/api/occupancy/by-department', methods=['GET'])
def get_occupancy_by_department():
    """
    Hent belægningsgrad per afdeling med benchmark
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        # Simplified occupancy calculation
        query_template = """
        SELECT 
            r.NAME as DEPARTMENT,
            COUNT(*) as TOTAL_BOOKINGS,
            SUM(CASE WHEN b.RESTAURANTBOOKING_B_STATUS = 'Confirmed' THEN 1 ELSE 0 END) as CONFIRMED_BOOKINGS,
            (SUM(CASE WHEN b.RESTAURANTBOOKING_B_STATUS = 'Confirmed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as OCCUPANCY_RATE
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        LEFT JOIN DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS r 
            ON b.RESTAURANTID = r.ID
        WHERE TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
          AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
        GROUP BY r.NAME
        ORDER BY OCCUPANCY_RATE DESC
        """
        
        current = query_snowflake(query_template.format(start_date, end_date))
        benchmark = query_snowflake(query_template.format(benchmark_start, benchmark_end))
        
        # Merge results
        result = []
        benchmark_dict = {item['DEPARTMENT']: item for item in benchmark}
        
        for dept in current:
            dept_name = dept['DEPARTMENT']
            benchmark_data = benchmark_dict.get(dept_name, {})
            
            current_rate = dept['OCCUPANCY_RATE'] or 0
            benchmark_rate = benchmark_data.get('OCCUPANCY_RATE', 0) or 0
            
            result.append({
                'department': dept_name,
                'current_rate': round(current_rate, 2),
                'benchmark_rate': round(benchmark_rate, 2),
                'change_points': round(current_rate - benchmark_rate, 2)
            })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/occupancy/by-category', methods=['GET'])
def get_occupancy_by_category():
    """
    Hent belægningsgrad per tidskategori og ugedag
    Parameters: start_date, end_date
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        query = """
        SELECT 
            CASE 
                WHEN HOUR(b.RESTAURANTBOOKING_B_DATE_TIME) BETWEEN 8 AND 11 THEN 'Formiddag'
                WHEN HOUR(b.RESTAURANTBOOKING_B_DATE_TIME) BETWEEN 12 AND 14 THEN 'Tidlig eftermiddag'
                WHEN HOUR(b.RESTAURANTBOOKING_B_DATE_TIME) BETWEEN 15 AND 17 THEN 'Eftermiddag'
                ELSE 'Aften'
            END as KATEGORI,
            DAYNAME(b.RESTAURANTBOOKING_B_DATE_TIME) as WEEKDAY,
            (SUM(CASE WHEN b.RESTAURANTBOOKING_B_STATUS = 'Confirmed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as OCCUPANCY_RATE
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        WHERE TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
          AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
        GROUP BY KATEGORI, WEEKDAY
        ORDER BY 
            CASE KATEGORI
                WHEN 'Formiddag' THEN 1
                WHEN 'Tidlig eftermiddag' THEN 2
                WHEN 'Eftermiddag' THEN 3
                WHEN 'Aften' THEN 4
            END,
            CASE WEEKDAY
                WHEN 'Mon' THEN 1
                WHEN 'Tue' THEN 2
                WHEN 'Wed' THEN 3
                WHEN 'Thu' THEN 4
                WHEN 'Fri' THEN 5
                WHEN 'Sat' THEN 6
                WHEN 'Sun' THEN 7
            END
        """.format(start_date, end_date)
        
        results = query_snowflake(query)
        
        # Format for heatmap
        formatted = []
        for row in results:
            formatted.append({
                'category': row['KATEGORI'],
                'weekday': row['WEEKDAY'],
                'occupancy_rate': round(row['OCCUPANCY_RATE'], 1)
            })
        
        return jsonify(formatted)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== OMSÆTNING (REVENUE) ====================

@app.route('/api/revenue/by-department', methods=['GET'])
def get_revenue_by_department():
    """
    Hent omsætning ex moms per afdeling fra SQL_PlecTo
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        # Current period - revenue from SQL_PlecTo synced to Snowflake
        query_current = """
        SELECT 
            Department as DEPARTMENT,
            SUM(TotalExclVAT) as REVENUE_EXCL_VAT
        FROM dbo.SQL_PlecTo
        WHERE CAST(Date AS DATE) >= ?
          AND CAST(Date AS DATE) <= ?
          AND SalesType = 'PosSale'
          AND (ItemGroupText NOT LIKE '%Gavekort%' 
               AND ItemGroupText NOT LIKE '%GiftUp%'
               AND ItemGroupText NOT LIKE '%Reklamationer%')
        GROUP BY Department
        ORDER BY SUM(TotalExclVAT) DESC
        """
        
        # Benchmark period
        query_benchmark = """
        SELECT 
            Department as DEPARTMENT,
            SUM(TotalExclVAT) as REVENUE_EXCL_VAT
        FROM dbo.SQL_PlecTo
        WHERE CAST(Date AS DATE) >= ?
          AND CAST(Date AS DATE) <= ?
          AND SalesType = 'PosSale'
          AND (ItemGroupText NOT LIKE '%Gavekort%' 
               AND ItemGroupText NOT LIKE '%GiftUp%'
               AND ItemGroupText NOT LIKE '%Reklamationer%')
        GROUP BY Department
        ORDER BY SUM(TotalExclVAT) DESC
        """
        
        current = query_sql_server(query_current, (start_date, end_date))
        benchmark = query_sql_server(query_benchmark, (benchmark_start, benchmark_end))
        
        # Merge current and benchmark
        result = []
        benchmark_dict = {item['DEPARTMENT']: item for item in benchmark if item['DEPARTMENT']}
        
        for dept in current:
            if not dept['DEPARTMENT']:
                continue
                
            dept_name = dept['DEPARTMENT']
            benchmark_data = benchmark_dict.get(dept_name, {})
            
            current_revenue = dept['REVENUE_EXCL_VAT'] or 0
            benchmark_revenue = benchmark_data.get('REVENUE_EXCL_VAT', 0) or 0
            
            change_percent = 0
            if benchmark_revenue > 0:
                change_percent = ((current_revenue - benchmark_revenue) / benchmark_revenue) * 100
            
            result.append({
                'department': dept_name,
                'current_revenue': round(current_revenue, 2),
                'benchmark_revenue': round(benchmark_revenue, 2),
                'change_percent': round(change_percent, 2)
            })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== LØNUDGIFTER (LABOR COSTS) ====================

@app.route('/api/labor/by-department', methods=['GET'])
def get_labor_by_department():
    """
    Hent lønudgifter per afdeling med benchmark
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        query_template = """
        SELECT 
            d.NAME as DEPARTMENT,
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME) * COALESCE(p.WAGE_RATE, 0)) as TOTAL_LABOR_COST,
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME)) as TOTAL_HOURS,
            COUNT(DISTINCT s.EMPLOYEEID) as EMPLOYEE_COUNT
        FROM PLANDAY.PYTHON_IMPORT.SHIFTS s
        LEFT JOIN PLANDAY.PYTHON_IMPORT.DEPARTMENTS d ON s.DEPARTMENTID = d.ID
        LEFT JOIN PLANDAY.PYTHON_IMPORT.PAYROLL p ON s.ID = p.ID
        WHERE DATE(s.STARTDATETIME) >= '{}'
          AND DATE(s.STARTDATETIME) <= '{}'
        GROUP BY d.NAME
        ORDER BY TOTAL_LABOR_COST DESC
        """
        
        current = query_snowflake(query_template.format(start_date, end_date))
        benchmark = query_snowflake(query_template.format(benchmark_start, benchmark_end))
        
        # Merge results
        result = []
        benchmark_dict = {item['DEPARTMENT']: item for item in benchmark if item['DEPARTMENT']}
        
        for dept in current:
            if not dept['DEPARTMENT']:
                continue
                
            dept_name = dept['DEPARTMENT']
            benchmark_data = benchmark_dict.get(dept_name, {})
            
            current_cost = dept['TOTAL_LABOR_COST'] or 0
            benchmark_cost = benchmark_data.get('TOTAL_LABOR_COST', 0) or 0
            
            change_percent = 0
            if benchmark_cost > 0:
                change_percent = ((current_cost - benchmark_cost) / benchmark_cost) * 100
            
            result.append({
                'department': dept_name,
                'current_cost': round(current_cost, 2),
                'benchmark_cost': round(benchmark_cost, 2),
                'current_hours': dept['TOTAL_HOURS'] or 0,
                'employee_count': dept['EMPLOYEE_COUNT'] or 0,
                'change_percent': round(change_percent, 2)
            })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/labor/summary', methods=['GET'])
def get_labor_summary():
    """
    Hent samlet lønudgifter summary
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        query_template = """
        SELECT 
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME) * COALESCE(p.WAGE_RATE, 0)) as TOTAL_COST,
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME)) as TOTAL_HOURS
        FROM PLANDAY.PYTHON_IMPORT.SHIFTS s
        LEFT JOIN PLANDAY.PYTHON_IMPORT.PAYROLL p ON s.ID = p.ID
        WHERE DATE(s.STARTDATETIME) >= '{}'
          AND DATE(s.STARTDATETIME) <= '{}'
        """
        
        current = query_snowflake(query_template.format(start_date, end_date))[0]
        benchmark = query_snowflake(query_template.format(benchmark_start, benchmark_end))[0]
        
        current_cost = current['TOTAL_COST'] or 0
        benchmark_cost = benchmark['TOTAL_COST'] or 0
        
        change_percent = 0
        if benchmark_cost > 0:
            change_percent = ((current_cost - benchmark_cost) / benchmark_cost) * 100
        
        return jsonify({
            'current_cost': round(current_cost, 2),
            'benchmark_cost': round(benchmark_cost, 2),
            'current_hours': current['TOTAL_HOURS'] or 0,
            'benchmark_hours': benchmark['TOTAL_HOURS'] or 0,
            'change_percent': round(change_percent, 2)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== OMSÆTNING VS PAX ====================

@app.route('/api/metrics/revenue-vs-pax', methods=['GET'])
def get_revenue_vs_pax():
    """
    Hent omsætning vs PAX per afdeling
    Parameters: start_date, end_date
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        # Get revenue data from SQL_PlecTo
        revenue_query = """
        SELECT 
            DEPARTMENT,
            SUM(TOTALEXCLVAT) as TOTAL_REVENUE
        FROM AJOUR.PYTHON_IMPORT.PLECTO
        WHERE TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
          AND TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
          AND SALESTYPE <> 'PosSaleTotal'
          AND (ITEMGROUPTEXT IS NULL OR ITEMGROUPTEXT NOT LIKE '%Gavekort%')
        GROUP BY DEPARTMENT
        """.format(start_date, end_date)
        
        # Get PAX data
        pax_query = """
        SELECT 
            r.NAME as DEPARTMENT,
            SUM(CAST(RESTAURANTBOOKING_B_PAX AS NUMBER)) as TOTAL_PAX
        FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
        LEFT JOIN DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS r ON b.RESTAURANTID = r.ID
        WHERE TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
          AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
        GROUP BY r.NAME
        """.format(start_date, end_date)
        
        revenue_data = query_snowflake(revenue_query)
        pax_data = query_snowflake(pax_query)
        
        # Merge data
        revenue_dict = {item['DEPARTMENT']: item['TOTAL_REVENUE'] for item in revenue_data if item['DEPARTMENT']}
        
        result = []
        for dept in pax_data:
            if dept['DEPARTMENT']:
                pax = dept['TOTAL_PAX'] or 0
                revenue = revenue_dict.get(dept['DEPARTMENT'], 0) or 0
                revenue_per_pax = (revenue / pax) if pax > 0 else 0
                
                result.append({
                    'department': dept['DEPARTMENT'],
                    'pax': pax,
                    'revenue': round(revenue, 2),
                    'revenue_per_pax': round(revenue_per_pax, 2)
                })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== LØN VS OMSÆTNING ====================

@app.route('/api/metrics/labor-vs-revenue', methods=['GET'])
def get_labor_vs_revenue():
    """
    Hent løn vs omsætning ratio per afdeling
    Parameters: start_date, end_date
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        # Get labor costs
        labor_query = """
        SELECT 
            d.NAME as DEPARTMENT,
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME) * COALESCE(p.WAGE_RATE, 0)) as LABOR_COST
        FROM PLANDAY.PYTHON_IMPORT.SHIFTS s
        LEFT JOIN PLANDAY.PYTHON_IMPORT.DEPARTMENTS d ON s.DEPARTMENTID = d.ID
        LEFT JOIN PLANDAY.PYTHON_IMPORT.PAYROLL p ON s.ID = p.ID
        WHERE DATE(s.STARTDATETIME) >= '{}'
          AND DATE(s.STARTDATETIME) <= '{}'
        GROUP BY d.NAME
        """.format(start_date, end_date)
        
        # Get revenue from SQL_PlecTo
        revenue_query = """
        SELECT 
            DEPARTMENT,
            SUM(TOTALEXCLVAT) as TOTAL_REVENUE
        FROM AJOUR.PYTHON_IMPORT.PLECTO
        WHERE TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
          AND TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
          AND SALESTYPE <> 'PosSaleTotal'
          AND (ITEMGROUPTEXT IS NULL OR ITEMGROUPTEXT NOT LIKE '%Gavekort%')
        GROUP BY DEPARTMENT
        """.format(start_date, end_date)
        
        labor_data = query_snowflake(labor_query)
        revenue_data = query_snowflake(revenue_query)
        
        # Merge data
        revenue_dict = {item['DEPARTMENT']: item['TOTAL_REVENUE'] for item in revenue_data if item['DEPARTMENT']}
        
        result = []
        for dept in labor_data:
            if dept['DEPARTMENT']:
                labor_cost = dept['LABOR_COST'] or 0
                revenue = revenue_dict.get(dept['DEPARTMENT'], 0) or 0
                labor_percentage = (labor_cost / revenue * 100) if revenue > 0 else 0
                
                result.append({
                    'department': dept['DEPARTMENT'],
                    'labor_cost': round(labor_cost, 2),
                    'revenue': round(revenue, 2),
                    'labor_percentage': round(labor_percentage, 2)
                })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== LØN VS PAX ====================

@app.route('/api/metrics/labor-vs-pax', methods=['GET'])
def get_labor_vs_pax():
    """
    Hent løn vs PAX per afdeling
    Parameters: start_date, end_date, benchmark_start, benchmark_end
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    benchmark_start = request.args.get('benchmark_start')
    benchmark_end = request.args.get('benchmark_end')
    
    try:
        # Combined query for labor and PAX
        query_template = """
        SELECT 
            d.NAME as DEPARTMENT,
            SUM(DATEDIFF(hour, s.STARTDATETIME, s.ENDDATETIME) * COALESCE(p.WAGE_RATE, 0)) as LABOR_COST,
            (
                SELECT SUM(CAST(RESTAURANTBOOKING_B_PAX AS NUMBER))
                FROM DINNERBOOKING.PYTHON_IMPORT.BOOKINGS b
                LEFT JOIN DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS r ON b.RESTAURANTID = r.ID
                WHERE r.NAME = d.NAME
                  AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') >= '{}'
                  AND TRY_TO_DATE(b.RESTAURANTBOOKING_B_DATE_TIME, 'MM/DD/YYYY HH12:MI:SS AM') <= '{}'
            ) as TOTAL_PAX
        FROM PLANDAY.PYTHON_IMPORT.SHIFTS s
        LEFT JOIN PLANDAY.PYTHON_IMPORT.DEPARTMENTS d ON s.DEPARTMENTID = d.ID
        LEFT JOIN PLANDAY.PYTHON_IMPORT.PAYROLL p ON s.ID = p.ID
        WHERE DATE(s.STARTDATETIME) >= '{}'
          AND DATE(s.STARTDATETIME) <= '{}'
        GROUP BY d.NAME
        """
        
        current = query_snowflake(query_template.format(
            start_date, end_date, start_date, end_date
        ))
        benchmark = query_snowflake(query_template.format(
            benchmark_start, benchmark_end, benchmark_start, benchmark_end
        ))
        
        # Merge and calculate
        result = []
        benchmark_dict = {item['DEPARTMENT']: item for item in benchmark if item['DEPARTMENT']}
        
        for dept in current:
            if not dept['DEPARTMENT']:
                continue
                
            dept_name = dept['DEPARTMENT']
            labor_cost = dept['LABOR_COST'] or 0
            pax = dept['TOTAL_PAX'] or 0
            
            labor_per_pax = 0
            if pax > 0:
                labor_per_pax = labor_cost / pax
            
            benchmark_data = benchmark_dict.get(dept_name, {})
            benchmark_labor = benchmark_data.get('LABOR_COST', 0) or 0
            benchmark_pax = benchmark_data.get('TOTAL_PAX', 0) or 0
            benchmark_per_pax = 0
            if benchmark_pax > 0:
                benchmark_per_pax = benchmark_labor / benchmark_pax
            
            change_percent = 0
            if benchmark_per_pax > 0:
                change_percent = ((labor_per_pax - benchmark_per_pax) / benchmark_per_pax) * 100
            
            result.append({
                'department': dept_name,
                'current_labor_per_pax': round(labor_per_pax, 2),
                'benchmark_labor_per_pax': round(benchmark_per_pax, 2),
                'change_percent': round(change_percent, 2)
            })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== EXPORT TIL CSV ====================

@app.route('/api/export/csv', methods=['POST'])
def export_to_csv():
    """
    Export data til CSV
    Body: { "endpoint": "/api/pax/by-department", "params": {...} }
    """
    try:
        data = request.get_json()
        endpoint = data.get('endpoint')
        params = data.get('params', {})
        
        # Call the specified endpoint internally
        # This is a simplified version - you might want to make actual internal calls
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers and data based on endpoint
        # This is a placeholder - customize based on actual data structure
        writer.writerow(['Column1', 'Column2', 'Column3'])
        writer.writerow(['Data1', 'Data2', 'Data3'])
        
        # Convert to bytes
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name='export.csv'
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== DEPARTMENTS LIST ====================

@app.route('/api/departments', methods=['GET'])
def get_departments():
    """Hent liste over alle afdelinger"""
    try:
        query = """
        SELECT DISTINCT NAME as DEPARTMENT
        FROM DINNERBOOKING.PYTHON_IMPORT.RESTAURANTS
        WHERE NAME IS NOT NULL
        ORDER BY NAME
        """
        
        results = query_snowflake(query)
        return jsonify([r['DEPARTMENT'] for r in results])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== RUN SERVER ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    app.run(host='0.0.0.0', port=port, debug=debug)
