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

import psycopg2
from psycopg2 import sql



config = configparser.ConfigParser()
config.read('../config.ini')

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
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    return conn

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
                cur.execute(query)
                created.append(name)
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

