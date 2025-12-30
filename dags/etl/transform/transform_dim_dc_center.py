import psycopg2
from psycopg2.extras import execute_values
from etl.common.checkpoint import CheckpointManager

# ---------- ETL DIM DISTRIBUTION CENTER (SCD TYPE 1) ----------
def etl_dim_dc(conn):
    cp = CheckpointManager()
    last_ts = cp.load("dc")
    cur = conn.cursor()

    # 1️⃣ Load trạng thái hiện tại từ Warehouse (SCD Type 1 - chỉ cần name hiện tại)
    cur.execute("SELECT dc_id, name FROM dw.dim_distribution_center")
    current_dim = {r[0]: r[1] for r in cur.fetchall()}
    print(f"🚀 Loaded {len(current_dim)} centers from Warehouse cache")

    # 2️⃣ Load CDC changes
    cur.execute("""
        SELECT id, name, ts_ms
        FROM public.distribution_centers
        WHERE ts_ms > %s AND op != 'd'
        ORDER BY ts_ms ASC
    """, (last_ts,))
    cdc_rows = cur.fetchall()
    print(f"✅ Found {len(cdc_rows)} new/updated records from source")

    to_upsert = {} 
    max_ts = last_ts

    # 3️⃣ Transform & Deduplicate
    for r in cdc_rows:
        dc_id, name, ts_ms = r
        max_ts = max(max_ts, ts_ms)

        # Chỉ đưa vào danh sách Upsert nếu có sự thay đổi so với Warehouse
        if current_dim.get(dc_id) != name:
            to_upsert[dc_id] = name

    # 4️⃣ Apply Batch Upsert
    if to_upsert:
        print(f"🔄 Upserting {len(to_upsert)} records...")
        upsert_data = [(k, v) for k, v in to_upsert.items()]
        
        execute_values(cur, """
            INSERT INTO dw.dim_distribution_center (dc_id, name)
            VALUES %s
            ON CONFLICT (dc_id) DO UPDATE SET 
                name = EXCLUDED.name
        """, upsert_data)

    cur.close()
    # Trả về max_ts và số lượng bản ghi đã xử lý cho Airflow/Main
    return max_ts, len(to_upsert)

# Cho phép chạy test độc lập
if __name__ == "__main__":
    from etl.common.database import get_dw_conn
    conn = get_dw_conn()
    try:
        ts, count = etl_dim_dc(conn)
        conn.commit()
        if ts > 0:
            CheckpointManager().save("dc", ts)
        print(f"🎉 DC ETL DONE | Upserted: {count}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        conn.close()