import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from etl.common.checkpoint import CheckpointManager

# ---------- ETL DIM PRODUCT (SCD TYPE 2) ----------
def etl_dim_product(conn):
    cp = CheckpointManager()
    last_ts = cp.load("products")
    cur = conn.cursor()

    print("🚀 Loading current dim_product cache...")
    cur.execute("SELECT product_id, product_sk, name, brand, category, department FROM dw.dim_product WHERE is_current = TRUE")
    current_dim = {r[0]: r[1:] for r in cur.fetchall()}

    cur.execute("SELECT id, name, brand, category, department, ts_ms FROM public.products WHERE ts_ms > %s AND op != 'd' ORDER BY ts_ms ASC", (last_ts,))
    cdc_rows = cur.fetchall()
    
    to_close, to_insert = [], []
    max_ts = last_ts

    for r in cdc_rows:
        p_id, name, brand, cat, dept, ts_ms = r
        valid_from = datetime.fromtimestamp(ts_ms / 1000).date()
        max_ts = max(max_ts, ts_ms)

        current = current_dim.get(p_id)
        new_val = (name, brand, cat, dept)
        
        if not current or current[1:] != new_val:
            if current: to_close.append((valid_from, current[0]))
            to_insert.append((p_id, name, brand, cat, dept, valid_from))

    if to_close:
        print(f"🔁 Closing {len(to_close)} old versions...")
        execute_values(cur, "UPDATE dw.dim_product SET is_current=FALSE, valid_to=v.v_to::date FROM (VALUES %s) AS v(v_to, sk) WHERE product_sk=v.sk::integer", to_close)
    
    if to_insert:
        print(f"➕ Inserting {len(to_insert)} new versions...")
        execute_values(cur, "INSERT INTO dw.dim_product (product_id, name, brand, category, department, valid_from, valid_to, is_current) VALUES %s", [(*r, None, True) for r in to_insert])

    cur.close()
    return max_ts, len(to_insert), len(to_close)

if __name__ == "__main__":
    from etl.common.database import get_dw_conn
    conn = get_dw_conn()
    try:
        ts, inc, cls = etl_dim_product(conn)
        conn.commit()
        if ts > 0: CheckpointManager().save("products", ts)
        print(f"🎉 PRODUCT ETL DONE | New: {inc} | Closed: {cls}")
    finally:
        conn.close()