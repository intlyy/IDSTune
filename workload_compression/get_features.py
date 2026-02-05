import psycopg2
import json
from typing import Dict, Any, List, Tuple


# -----------------------------
# Task-specific feature extractors
# -----------------------------

def _fetch_db_exec_stats(cur) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            datname,
            xact_commit, xact_rollback,
            blks_read, blks_hit,
            tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database();
        """
    )
    row = cur.fetchone()
    if not row:
        return {}
    blocks_read = row[3]
    blocks_hit = row[4]
    return {
        'blocks_read': blocks_read,
        'blocks_hit': blocks_hit,
        'tuples_returned': row[5],
        'tuples_fetched': row[6],
        'tuples_inserted': row[7],
        'tuples_updated': row[8],
        'tuples_deleted': row[9],
        'buffer_pool_hit_ratio': blocks_hit / (blocks_read + blocks_hit + 1e-9),
        'xact_commit': row[1],
        'xact_rollback': row[2],
    }


def _fetch_tables(cur) -> List[Tuple]:
    cur.execute(
        """
        SELECT
            c.relname AS table_name,
            c.reltuples::BIGINT AS est_rows,
            c.relpages AS pages,
            pg_total_relation_size(c.oid) AS total_bytes,
            n.nspname AS schema_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r';
        """
    )
    return cur.fetchall()


def _fetch_columns_stats(cur) -> List[Tuple]:
    cur.execute(
        """
        SELECT
            tablename,
            attname,
            null_frac,
            n_distinct,
            avg_width,
            most_common_vals,
            histogram_bounds
        FROM pg_stats
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
        """
    )
    return cur.fetchall()


def _fetch_indexes(cur) -> List[Tuple]:
    cur.execute(
        """
        SELECT
            t.relname AS table_name,
            i.relname AS index_name,
            a.attname AS column_name,
            ix.indisunique,
            ix.indisprimary
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid
                          AND a.attnum = ANY(ix.indkey)
        WHERE t.relkind = 'r';
        """
    )
    return cur.fetchall()


def _fetch_index_usage(cur) -> List[Tuple]:
    # Optional: index usage stats (standard view)
    cur.execute(
        """
        SELECT
            c.relname AS table_name,
            s.indexrelname AS index_name,
            s.idx_scan,
            s.idx_tup_read,
            s.idx_tup_fetch
        FROM pg_stat_user_indexes s
        JOIN pg_class c ON s.relid = c.oid;
        """
    )
    return cur.fetchall()


def _fetch_top_queries(cur, limit: int = 100) -> List[Tuple]:
    cur.execute(
        """
        SELECT query, calls, rows, total_exec_time
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return cur.fetchall()


def extract_features_indexes_recommendation(conn_str: str) -> Dict[str, Any]:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        tables = _fetch_tables(cur)
        cols = _fetch_columns_stats(cur)
        indexes = _fetch_indexes(cur)
        usage = _fetch_index_usage(cur)

        # Build per-table summary focused on schema + index signals
        table_features = []
        for t in tables:
            table_name = t[0]
            est_rows = t[1]
            table_cols = [c for c in cols if c[0] == table_name]
            columns = {c[1]: {'n_distinct': c[3], 'null_frac': c[2], 'avg_width': c[4]} for c in table_cols}
            table_indexes = {idx[1] for idx in indexes if idx[0] == table_name}
            index_usage = [
                {
                    'index': u[1],
                    'idx_scan': u[2],
                    'idx_tup_read': u[3],
                    'idx_tup_fetch': u[4],
                }
                for u in usage if u[0] == table_name
            ]
            table_features.append({
                'table': table_name,
                'est_rows': est_rows,
                'columns': columns,
                'indexes': sorted(list(table_indexes)),
                'index_usage': index_usage,
            })

        return {
            'task': 'indexes_recommendation',
            'tables': table_features,
        }
    finally:
        cur.close()
        conn.close()


def extract_features_materialised_views_recommendation(conn_str: str) -> Dict[str, Any]:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        tables = _fetch_tables(cur)
        queries = _fetch_top_queries(cur, limit=100)

        tables_overview = [
            {
                'table': t[0],
                'est_rows': t[1],
                'pages': t[2],
                'total_bytes': t[3],
                'schema': t[4],
            }
            for t in tables
        ]

        top_queries = [
            {'query': q[0], 'calls': q[1], 'rows': q[2], 'total_exec_time_ms': q[3]}
            for q in queries
        ]

        return {
            'task': 'materialised_views_recommendation',
            'tables_overview': tables_overview,
            'top_queries': top_queries,
        }
    finally:
        cur.close()
        conn.close()


def extract_features_knob_tuning(conn_str: str) -> Dict[str, Any]:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        exec_stats = _fetch_db_exec_stats(cur)
        return {
            'task': 'knob_tuning',
            'execution': exec_stats,
        }
    finally:
        cur.close()
        conn.close()


def extract_features_optimization_plan_review(conn_str: str) -> Dict[str, Any]:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        queries = _fetch_top_queries(cur, limit=100)
        top_queries = [
            {
                'query': q[0],
                'calls': q[1],
                'rows': q[2],
                'total_exec_time_ms': q[3],
            }
            for q in queries
        ]
        return {
            'task': 'optimization_plan_review',
            'top_queries': top_queries,
        }
    finally:
        cur.close()
        conn.close()


# -----------------------------
# Public dispatcher
# -----------------------------

_TASK_IMPL = {
    'indexes_recommendation': extract_features_indexes_recommendation,
    'indexes recommendation': extract_features_indexes_recommendation,
    'materialised_views_recommendation': extract_features_materialised_views_recommendation,
    'materialised views recommendation': extract_features_materialised_views_recommendation,
    'knob_tuning': extract_features_knob_tuning,
    'knob tuning': extract_features_knob_tuning,
    'optimization_plan_review': extract_features_optimization_plan_review,
    'optimization plan review': extract_features_optimization_plan_review,
}


def extract_features(conn_str: str, task: str) -> Dict[str, Any]:
    key = task.strip().lower()
    func = _TASK_IMPL.get(key)
    if func is None:
        raise ValueError(f"Unknown task '{task}'. Supported: {sorted(_TASK_IMPL.keys())}")
    return func(conn_str)


def reset_pgstat_statements(conn_str: str) -> None:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_stat_statements_reset();")
        conn.commit()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    conn_str = "dbname= user= password= host= port=5432"
    tasks = [
        "indexes_recommendation",
        "materialised_views_recommendation",
        "knob_tuning",
        "optimization_plan_review",
    ]
    for t in tasks:
        feats = extract_features(conn_str, t)
        outfile = f"{t.replace(' ', '_')}_features.json"
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(feats, f, ensure_ascii=False, indent=2)

