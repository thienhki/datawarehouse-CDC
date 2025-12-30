import psycopg2
from psycopg2.extras import execute_values

DW_CONN_PARAMS = {
    'host': 'localhost',
    'port': 5433,
    'database': 'E_COMMERCE',
    'user': 'dovanthien',
    'password': '210404'
}

def get_dw_conn():
    return psycopg2.connect(**DW_CONN_PARAMS)

def etl_dim_status():
    conn = get_dw_conn()
    cur = conn.cursor()

    # Lấy tất cả các status duy nhất từ bảng orders gốc
    cur.execute("SELECT DISTINCT status FROM public.orders WHERE status IS NOT NULL")
    statuses = [r[0] for r in cur.fetchall()]

    if statuses:
        # Nạp vào dim_status, nếu trùng tên status thì không làm gì cả
        # status_sk nên để tự tăng (SERIAL/IDENTITY)
        execute_values(cur, """
            INSERT INTO dw.dim_status (status)
            VALUES %s
            ON CONFLICT (status) DO NOTHING
        """, [(s,) for s in statuses])

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Cập nhật dim_status hoàn tất")

etl_dim_status()