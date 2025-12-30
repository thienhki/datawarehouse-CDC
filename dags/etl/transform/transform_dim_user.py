import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from etl.common.checkpoint import CheckpointManager

# ---------- HELPERS ----------
def normalize_gender(g):
    if g == 'M': return 'Male'
    if g == 'F': return 'Female'
    return 'Unknown'

def age_group(age):
    if age is None: return 'Unknown'
    if age < 30: return "Young"
    if age < 55: return "Middle"
    return "Senior"

# ---------- ETL DIM USER ----------
def etl_dim_user(conn):
    cp = CheckpointManager()
    last_ts = cp.load("users")
    cur = conn.cursor()

    # 1️⃣ Load trạng thái hiện tại
    print("🚀 Loading current dim_user cache...")
    cur.execute("""
        SELECT user_id, user_sk, gender, age, age_group, traffic_source, city, state, country
        FROM dw.dim_user WHERE is_current = TRUE
    """)
    current_dim = {r[0]: r[1:] for r in cur.fetchall()}
    print(f"✅ Loaded {len(current_dim)} active users")

    # 2️⃣ Load CDC changes
    print("🚀 Fetching CDC changes...")
    cur.execute("""
        SELECT id, gender, age, traffic_source, city, state, country, ts_ms
        FROM public.users
        WHERE ts_ms > %s AND op != 'd'
        ORDER BY ts_ms ASC
    """, (last_ts,))
    cdc_rows = cur.fetchall()
    print(f"✅ Found {len(cdc_rows)} records")
    
    to_close = []
    to_insert = []
    max_ts = last_ts

    # 3️⃣ Transform
    for r in cdc_rows:
        u_id, gender, age, traffic_source, city, state, country, ts_ms = r
        valid_from = datetime.fromtimestamp(ts_ms / 1000).date()
        max_ts = max(max_ts, ts_ms)

        g_norm = normalize_gender(gender)
        a_grp = age_group(age)
        new_val = (g_norm, age, a_grp, traffic_source, city, state, country)

        current = current_dim.get(u_id)
        
        if not current or current[1:] != new_val:
            if current: 
                to_close.append((valid_from, current[0]))
            to_insert.append((u_id, g_norm, age, a_grp, traffic_source, city, state, country, valid_from))

    # 4️⃣ Apply Batch Updates
    if to_close:
        print(f"🔁 Closing {len(to_close)} old versions...")
        execute_values(cur, """
            UPDATE dw.dim_user SET is_current = FALSE, valid_to = v.v_to::date
            FROM (VALUES %s) AS v(v_to, sk) WHERE user_sk = v.sk::integer
        """, to_close)

    if to_insert:
        print(f"➕ Inserting {len(to_insert)} new versions...")
        execute_values(cur, """
            INSERT INTO dw.dim_user 
            (user_id, gender, age, age_group, traffic_source, city, state, country, valid_from, valid_to, is_current)
            VALUES %s
        """, [(*r, None, True) for r in to_insert])
    
    # QUAN TRỌNG: Không đóng conn ở đây nếu dùng Airflow điều phối
    cur.close()

    # Trả về các thông số để DAG xử lý tiếp (commit, save checkpoint, logging)
    return max_ts, len(to_insert), len(to_close)

# Cho phép chạy độc lập để test
if __name__ == "__main__":
    from etl.common.database import get_dw_conn
    connection = get_dw_conn()
    try:
        m_ts, inc, cls = etl_dim_user(connection)
        connection.commit()
        if m_ts > 0:
            CheckpointManager().save("users", m_ts)
        print(f"🎉 TEST DONE | New: {inc} | Closed: {cls}")
    finally:
        connection.close()