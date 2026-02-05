import requests
import json
import pymysql
import os
import sys
import time
import re
import paramiko
import configparser
import subprocess
from typing import Dict, Any, List, Optional
import shutil
import psycopg2
from psycopg2 import sql



config = configparser.ConfigParser()
config.read('../config.ini')

def restore_postgres_config():
    # --- Configuration paths ---
    # Confirm these paths for your environment
    PG_VERSION = "15"
    CLUSTER_NAME = "main"
    
    BASE_DIR = f"/etc/postgresql/{PG_VERSION}/{CLUSTER_NAME}"
    DATA_DIR = f"/var/lib/postgresql/{PG_VERSION}/{CLUSTER_NAME}"
    
    CONF_FILE = os.path.join(BASE_DIR, "postgresql.conf")
    BACKUP_FILE = os.path.join(BASE_DIR, "postgresql.conf.bak")
    AUTO_CONF_FILE = os.path.join(DATA_DIR, "postgresql.auto.conf")
    
    SERVICE_NAME = f"postgresql@{PG_VERSION}-{CLUSTER_NAME}.service"

    print("[*] Starting PostgreSQL configuration restore...")

    if os.geteuid() != 0:
        print("[!] Error: this script requires root privileges to modify system configuration and restart the service.")
        print("    Please run with 'sudo python3 your_script.py'.")
        return False

    if not os.path.exists(BACKUP_FILE):
        print(f"[!] Error: backup file does not exist: {BACKUP_FILE}")
        return False
    
    try:
        print(f"[-] Restoring from backup: {CONF_FILE}")
        shutil.copy2(BACKUP_FILE, CONF_FILE)
        subprocess.run(["chown", "postgres:postgres", CONF_FILE], check=True)
        
    except Exception as e:
        print(f"[!] Failed to restore configuration file: {e}")
        return False

    if os.path.exists(AUTO_CONF_FILE):
        try:
            print(f"[-] Detected auto.conf, removing: {AUTO_CONF_FILE}")
            os.remove(AUTO_CONF_FILE)
        except OSError as e:
            print(f"[!] Unable to remove auto.conf: {e}")

    print(f"[-] Restarting service: {SERVICE_NAME} ...")
    try:
        result = subprocess.run(
            ["systemctl", "restart", SERVICE_NAME],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("[+] Restore succeeded. Database restarted and running.")
            return True
        else:
            print(f"[!] Restart failed. Systemd output:\n{result.stderr}")
            return False
            
    except Exception as e:
        print(f"[!] Failed to invoke systemctl: {e}")
        return False

def drop_all_materialized_views():
    """
    Drop all materialized views in the public schema.
    """
    print("[*] Scanning and dropping all materialized views...")
    conn = _get_pg_connection()
    with conn.cursor() as cur:
        # Query all materialized views
        cur.execute("""
            SELECT schemaname, matviewname 
            FROM pg_matviews 
            WHERE schemaname = 'public';
        """)
        mvs = cur.fetchall()

        if not mvs:
            print("    - No materialized views found.")
            return

        for schema, name in mvs:
            # Use CASCADE in case indexes depend on it
            drop_query = f'DROP MATERIALIZED VIEW IF EXISTS "{schema}"."{name}" CASCADE;'
            print(f"    - Dropping materialized view: {name}")
            cur.execute(drop_query)
        
    conn.commit()
    print("[+] All materialized views have been dropped.")

def reset_indexes_to_original(allowed_indexes=ORIGINAL_INDEXES):
    """
    Drop all indexes except those in 'allowed_indexes' and primary keys.
    
    Args:
        conn: database connection object
        allowed_indexes: set of index names to keep
    """
    print("[*] Scanning and removing extra indexes...")
    conn = _get_pg_connection()
    dropped_count = 0
    with conn.cursor() as cur:
        # Query all indexes, excluding primary keys
        # We usually do not want to drop primary keys because that would break table structure
        cur.execute("""
            SELECT 
                schemaname, 
                indexname 
            FROM pg_indexes 
            WHERE schemaname = 'public'
            -- Simple filter for primary keys (usually ends with _pkey)
            -- A stricter approach is to join pg_constraint, but this is often sufficient for tuning
            AND indexname NOT LIKE '%_pkey' 
            AND indexname NOT LIKE '%_unique';
        """)
        
        current_indexes = cur.fetchall()

        for schema, index_name in current_indexes:
            # Drop the index if it is not in the allow list
            if index_name not in allowed_indexes:
                print(f"    - Dropping extra index: {index_name}")
                cur.execute(f'DROP INDEX IF EXISTS "{schema}"."{index_name}";')
                dropped_count += 1
            else:
                # This is an original index; keep it
                pass

    conn.commit()
    if dropped_count == 0:
        print("    - No extra indexes found; index state is clean.")
    else:
        print(f"[+] Dropped {dropped_count} extra indexes; restored to original state.")

def _load_pg_conn_params():
    section = 'configuration recommender'
    host =  config.get(section, 'PG_Host')
    port =  config.get(section, 'PG_Port', fallback='5432')
    user =  config.get(section, 'PG_User', fallback='postgres')
    password =  config.get(section, 'PG_Password', fallback='')
    database =  config.get(section, 'PG_DB',  fallback='postgres')
    params = {'host': host, 'port': port, 'user': user, 'password': password, 'dbname': database}
    return params

def _get_pg_connection():
    params = _load_pg_conn_params()
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to database after {max_retries} attempts")
                print(f"Attempting to restore database configuration...")
                restore_postgres_config()
                # Try one more connection after restore
                try:
                    conn = psycopg2.connect(**params)
                    conn.autocommit = True
                    print("Connection successful after restoring configuration")
                    return conn
                except psycopg2.OperationalError as e2:
                    print(f"Connection still failed after restoration: {e2}")
                    raise

def _sanitize_guc_name(name: str) -> str:
    if not re.match(r'^[a-zA-Z0-9_.]+$', name):
        raise ValueError(f'Invalid GUC parameter name: {name}')
    return name

def apply_pg_knobs(plan_knobs) -> List[str]:
    if not plan_knobs:
        return []
    conn = _get_pg_connection()
    cur = conn.cursor()
    notes: List[str] = []
    try:
        for knob_name, meta in plan_knobs.items():
            param = _sanitize_guc_name(knob_name)
            value = meta.get('value') if isinstance(meta, dict) else meta
            try:
                cur.execute(f"ALTER SYSTEM SET {param} = %s", (str(value),))
            except Exception:
                try:
                    cur.execute(f"SET {param} = %s", (str(value),))
                    notes.append(f"SET applied for {param}; not persisted")
                except Exception as e2:
                    notes.append(f"Failed to set {param}: {e2}")
        try:
            cur.execute("SELECT pg_reload_conf();")
        except Exception as e:
            notes.append(f"pg_reload_conf failed: {e}")

        # Restart PostgreSQL to make all parameters take effect
        try:
            subprocess.run(["sudo", "systemctl", "restart", "postgresql"], check=True)
            notes.append("PostgreSQL restarted successfully")
            time.sleep(5)  # Wait for the database restart to complete
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to restart PostgreSQL: {e}"
            notes.append(error_msg)
            raise RuntimeError(error_msg)
    finally:
        cur.close()
        conn.close()
    return notes

def ensure_indexes(indexes: List[Dict[str, Any]]) -> List[str]:
    if not indexes:
        return []
    conn = _get_pg_connection()
    cur = conn.cursor()
    created = []
    try:
        for idx in indexes:
            name = idx.get('name')
            table = idx.get('table')
            cols = idx.get('columns', [])
            if not name or not table or not cols:
                continue
            try:
                if sql:
                    cols_sql = sql.SQL(', ').join(sql.Identifier(c) for c in cols)
                    cur.execute(sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(name), sql.Identifier(table), cols_sql
                    ))
                else:
                    cur.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{name}\" ON \"{table}\" ({', '.join(cols)})")
            except Exception:
                # fallback without concurrently
                if sql:
                    cols_sql = sql.SQL(', ').join(sql.Identifier(c) for c in cols)
                    cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(name), sql.Identifier(table), cols_sql
                    ))
                else:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS \"{name}\" ON \"{table}\" ({', '.join(cols)})")
            created.append(name)
    finally:
        cur.close()
        conn.close()
    return created

def ensure_matviews(matviews: List[Dict[str, Any]]) -> List[str]:
    if not matviews:
        return []
    conn = _get_pg_connection()
    cur = conn.cursor()
    created = []
    try:
        for mv in matviews:
            name = mv.get('name')
            query = mv.get('query')
            if not name or not query:
                continue
            try:
                cur.execute("SELECT 1 FROM pg_matviews WHERE schemaname = current_schema() AND matviewname = %s", (name,))
                exists = cur.fetchone() is not None
            except Exception:
                exists = False
            if not exists:
                # Ensure query is wrapped in CREATE MATERIALIZED VIEW statement
                if not query.strip().upper().startswith('CREATE'):
                    create_stmt = f"CREATE MATERIALIZED VIEW {name} AS {query}"
                else:
                    create_stmt = query
                try:
                    cur.execute(create_stmt)
                    created.append(name)
                    print(f"Created materialized view: {name}")
                except Exception as e:
                    print(f"Failed to create materialized view {name}: {e}")
    finally:
        cur.close()
        conn.close()
    return created

def _collect_sql_files(query_dir: str) -> List[str]:
    if not query_dir or not os.path.isdir(query_dir):
        return []
    return [os.path.join(query_dir, f) for f in os.listdir(query_dir) if f.endswith('.sql')]

def _run_sql_file(conn, sql_path: str) -> float:
    start = time.time()
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_text = f.read()
    cur = conn.cursor()
    try:
        cur.execute(sql_text)
    except Exception:
        for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
            cur.execute(stmt)
    finally:
        cur.close()
    end = time.time()
    return end - start

def test_by_job(plan: Dict[str, Any], query_dir: Optional[str] = None, log_file: Optional[str] = None) -> float:
    # PostgreSQL version: apply knobs/indexes/matviews, then run SQL files in JOB workload
    apply_pg_knobs(plan.get('knobs', {}) )
    ensure_indexes(plan.get('indexes', []))
    ensure_matviews(plan.get('matviews', []))

    if not query_dir:
        query_dir = os.getenv('JOB_QUERY_DIR', '')
    query_files = _collect_sql_files(query_dir)
    if not query_files:
        print('No JOB queries found; please set query_dir or JOB_QUERY_DIR')
        return -1.0

    conn = _get_pg_connection()
    total_time = 0.0
    try:
        for query_file in query_files:
            elapsed_time = _run_sql_file(conn, query_file)
            if log_file:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{os.path.basename(query_file)}: {elapsed_time:.4f}s\n")
            total_time += elapsed_time
    finally:
        conn.close()
    return total_time

    
def test_by_tpcc(plan: Dict[str, Any],  clients: int = 32, duration: int = 120, report_interval: int = 60) -> float:
    # Apply changes then run pgbench as a stand-in workload and parse TPS
    apply_pg_knobs(plan.get('knobs', {}))
    ensure_indexes(plan.get('indexes', []))
    ensure_matviews(plan.get('matviews', []))

    params = _load_pg_conn_params()
    env = os.environ.copy()
    if params.get('password'):
        env['PGPASSWORD'] = params['password']
    cmd = [
        'pgbench', '-h', str(params['host']), '-p', str(params['port']), '-U', str(params['user']),
        '-d', str(params['dbname']), '-c', str(clients), '-T', str(duration), '-P', str(report_interval)
    ]
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, check=False)
        output = proc.stdout
        # Parse "tps = 123.45" line
        tps_vals = [float(m.group(1)) for m in re.finditer(r"tps\s*=\s*([0-9.]+)", output)]
        if tps_vals:
            return float(sum(tps_vals) / len(tps_vals))
        # Fallback: parse per second from transactions line
        m = re.search(r"transactions:.*?\(([^)]+) per sec\.", output)
        if m:
            try:
                return float(m.group(1).split()[0])
            except Exception:
                pass
        return 0.0
    except FileNotFoundError:
        print('pgbench not found in PATH')
        return 0.0
    

def test_by_sysbench(plan: Dict[str, Any], threads: int = 32, duration: int = 120, report_interval: int = 60, tables: int = 50, table_size: int = 1000000, log_file: Optional[str] = None) -> float:
    # Apply changes then run sysbench (pgsql) and parse TPS
    apply_pg_knobs(plan.get('knobs', {}))
    ensure_indexes(plan.get('indexes', []))
    ensure_matviews(plan.get('matviews', []))

    params = _load_pg_conn_params()
    command = [
        'sysbench', '--db-driver=pgsql', f'--threads={threads}', f'--pgsql-host={params["host"]}', f'--pgsql-port={params["port"]}',
        f'--pgsql-user={params["user"]}', f'--pgsql-password={params["password"]}', f'--pgsql-db={params["dbname"]}',
        f'--tables={tables}', f'--table-size={table_size}', f'--time={duration}', f'--report-interval={report_interval}', 'oltp_read_write', 'run'
    ]
    try:
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        output = proc.stdout
        if log_file:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(output)
        # Parse TPS from "transactions:" or per-interval lines
        tps_vals = [float(m.group(1)) for m in re.finditer(r"transactions:\s*\d+\s*\(([^)]+) per sec\.", output)]
        if tps_vals:
            return float(sum(tps_vals) / len(tps_vals))
        # Fallback: compute from qps if present (heuristic)
        qps_vals = [float(m.group(1)) for m in re.finditer(r"queries:\s*\d+\s*\(([^)]+) per sec\.", output)]
        if qps_vals:
            qps = sum(qps_vals) / len(qps_vals)
            return qps / 20.0
        return 0.0
    except FileNotFoundError:
        print('sysbench not found in PATH')
        return 0.0

def unknown_benchmark(name):
    print(f"Unknown benchmark: {name}")

def test_by_tpcds(plan: Dict[str, Any], query_dir: Optional[str] = None, log_file: Optional[str] = None) -> float:
    # PostgreSQL version: apply knobs/indexes/matviews, then run TPC-DS SQL files
    apply_pg_knobs(plan.get('knobs', {}))
    ensure_indexes(plan.get('indexes', []))
    ensure_matviews(plan.get('matviews', []))

    if not query_dir:
        query_dir = os.getenv('TPCDS_QUERY_DIR', '')
    query_files = _collect_sql_files(query_dir)
    if not query_files:
        print('No TPC-DS queries found; please set query_dir or TPCDS_QUERY_DIR')
        return -1.0

    conn = _get_pg_connection()
    total_time = 0.0
    try:
        for query_file in query_files:
            elapsed_time = _run_sql_file(conn, query_file)
            if log_file:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{os.path.basename(query_file)}: {elapsed_time:.4f}s\n")
            total_time += elapsed_time
    finally:
        conn.close()
    return total_time

