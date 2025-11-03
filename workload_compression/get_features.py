import psycopg2
import json

def extract_features(conn_str):
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    features = {}

    # -------------------------
    # 1. Execution Features
    # -------------------------
    cur.execute("""
        SELECT
            datname,
            xact_commit, xact_rollback,
            blks_read, blks_hit,
            tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database();
    """)
    db_stat = cur.fetchone()
    features['execution'] = {
        'blocks_read': db_stat[3],
        'blocks_hit': db_stat[4],
        'tuples_returned': db_stat[5],
        'tuples_fetched': db_stat[6]
    }

    # Buffer pool hit ratio
    features['execution']['buffer_pool_hit_ratio'] = \
        db_stat[4] / (db_stat[3] + db_stat[4] + 1e-9)

    # -------------------------
    # 2. Schema & Data Features
    # -------------------------
    cur.execute("""
        SELECT
            c.relname AS table_name,
            c.reltuples::BIGINT AS est_rows,
            c.relpages AS pages,
            pg_total_relation_size(c.oid) AS total_bytes,
            n.nspname AS schema_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r';
    """)
    tables = cur.fetchall()
    features['tables'] = []
    # for t in tables:
    #     features['tables'].append({
    #         'table': t[0],
    #         'est_rows': t[1]
    #     })

    # Column-level features from pg_stats
    cur.execute("""
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
    """)
    cols = cur.fetchall()
    # features['columns'] = []
    # for c in cols:
    #     features['columns'].append({
    #         'table': c[0],
    #         'column': c[1],
    #         'n_distinct': c[3]
    #     })

    # -------------------------
    # 3. Index Features
    # -------------------------
    cur.execute("""
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
    """)
    indexes = cur.fetchall()
    # features['indexes'] = []
    # for idx in indexes:
    #     features['indexes'].append({
    #         'table': idx[0],
    #         'index': idx[1]
    #     })

    for t in tables:
        table_name = t[0]
        est_rows = t[1]

        # 列信息
        table_cols = [c for c in cols if c[0] == table_name]
        columns = {c[1]: {'n_distinct': c[3]} for c in table_cols}  # column_name -> {n_distinct: value}

        # 索引信息（去重）
        table_indexes = {idx[1] for idx in indexes if idx[0] == table_name}

        features['tables'].append({
            'table': table_name,
            'est_rows': est_rows,
            'columns': columns,          # {column_name: {'n_distinct': value}}
            'indexes': list(table_indexes)  # 去重索引名列表
        })

    # -------------------------
    # 4. Query-Access Features (need pg_stat_statements)
    # -------------------------
    cur.execute("""
        SELECT query, calls, rows, total_exec_time
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 100;
    """)
    queries = cur.fetchall()
    features['queries'] = []
    for q in queries:
        features['queries'].append({
            'query': q[0],
            'exec_time_ms': q[3]
        })

    cur.close()
    conn.close()

    return features


if __name__ == "__main__":
    conn_str = "dbname= user= password= host= port=5432"
    task = ["indexes_recommendation", "materialised_views_recommendation", "knob_tuning", "optimization_plan_review"]
    feats = extract_features(conn_str)
    with open("{task}_features.json", "w", encoding="utf-8") as f:
        json.dump(feats, f, ensure_ascii=False, indent=2)
