import psycopg2
from psycopg2.extras import execute_values
from etl.common.checkpoint import CheckpointManager

def etl_fact_sales(conn):
    cp = CheckpointManager()
    last_ts = cp.load("fact_sales")
    cur = conn.cursor()

    print(f"🚀 Fetching CDC items from ts_ms > {last_ts}...")
    
    # 1️⃣ SQL Query: Join Source & Warehouse Dimensions
    query = """
        SELECT 
            oi.id AS sales_id,
            p.product_sk,
            u.user_sk,
            d.date_sk,
            dc.dc_sk,
            s.status_sk,
            1 AS quantity,
            oi.sale_price,
            (oi.sale_price * 1) AS revenue,
            COALESCE(ii.cost, 0) AS cost,
            (oi.sale_price - COALESCE(ii.cost, 0)) AS gross_profit,
            oi.ts_ms
        FROM public.order_items oi
        INNER JOIN public.inventory_items ii ON oi.inventory_item_id = ii.id
        INNER JOIN public.orders o ON oi.order_id = o.order_id
        LEFT JOIN dw.dim_product p ON ii.product_id = p.product_id 
            AND (p.valid_from <= oi.created_at::DATE AND (p.valid_to > oi.created_at::DATE OR p.valid_to IS NULL))
        LEFT JOIN dw.dim_user u ON o.user_id = u.user_id 
            AND (u.valid_from <= oi.created_at::DATE AND (u.valid_to > oi.created_at::DATE OR u.valid_to IS NULL))
        LEFT JOIN dw.dim_date d ON oi.created_at::DATE = d.full_date
        LEFT JOIN dw.dim_status s ON oi.status = s.status
        LEFT JOIN dw.dim_distribution_center dc ON ii.distribution_center_id = dc.dc_id
        WHERE oi.ts_ms > %s
        ORDER BY oi.ts_ms ASC
    """
    cur.execute(query, (last_ts,))
    rows = cur.fetchall()
    
    if not rows:
        print("☕ Không có dữ liệu mới.")
        cur.close()
        return last_ts, 0

    print(f"✅ Found {len(rows)} raw records from source")

    # 2️⃣ Deduplicate (Chỉ lấy trạng thái mới nhất của sales_id trong batch này)
    unique_records = {}
    max_ts = last_ts

    for r in rows:
        sales_id = r[0]
        ts_ms = r[11]
        
        unique_records[sales_id] = (
            sales_id, r[1], r[2], r[3], r[4], r[5], # SKs
            r[6], r[7], r[8], r[9], r[10]           # Measures
        )
        if ts_ms > max_ts:
            max_ts = ts_ms

    to_insert = list(unique_records.values())
    
    # 3️⃣ Batch Upsert (ON CONFLICT)
    if to_insert:
        print(f"➕ Upserting {len(to_insert)} records into dw.fact_sales")
        insert_sql = """
            INSERT INTO dw.fact_sales (
                sales_id, product_sk, user_sk, date_sk, dc_sk, status_sk, 
                quantity, sale_price, revenue, cost, gross_profit
            ) VALUES %s
            ON CONFLICT (sales_id) DO UPDATE SET
                product_sk = EXCLUDED.product_sk,
                user_sk = EXCLUDED.user_sk,
                status_sk = EXCLUDED.status_sk,
                sale_price = EXCLUDED.sale_price,
                revenue = EXCLUDED.revenue,
                cost = EXCLUDED.cost,
                gross_profit = EXCLUDED.gross_profit;
        """
        execute_values(cur, insert_sql, to_insert)
    
    cur.close()
    return max_ts, len(to_insert)

# Cho phép chạy test độc lập
if __name__ == "__main__":
    from etl.common.database import get_dw_conn
    conn = get_dw_conn()
    try:
        ts, count = etl_fact_sales(conn)
        conn.commit()
        if ts > 0:
            CheckpointManager().save("fact_sales", ts)
        print(f"🎉 FACT ETL DONE | Total items: {count}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        conn.close()