import psycopg2

def dim_date():
    try:
        conn = psycopg2.connect(
            host='localhost', port=5433, database='E_COMMERCE',
            user='dovanthien', password='210404'
        )
        cur = conn.cursor()

        # Script SQL tổng hợp: Xóa -> Tạo -> Nạp -> Thêm dòng -1
        sql_script = """
        DROP TABLE IF EXISTS dw.dim_date CASCADE;
        
        CREATE TABLE dw.dim_date AS
        SELECT 
            TO_CHAR(d, 'YYYYMMDD')::INT AS date_sk,
            d::DATE AS full_date,
            EXTRACT(DAY FROM d)::INT AS day,
            EXTRACT(MONTH FROM d)::INT AS month,
            EXTRACT(YEAR FROM d)::INT AS year,
            EXTRACT(QUARTER FROM d)::INT AS quarter,
            TO_CHAR(d, 'Month') AS month_name,
            TO_CHAR(d, 'Day') AS day_name,
            EXTRACT(ISODOW FROM d)::INT AS day_of_week,
            (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
        FROM generate_series('2019-01-01'::DATE, '2035-12-31'::DATE, '1 day'::interval) d;

        ALTER TABLE dw.dim_date ADD PRIMARY KEY (date_sk);

        INSERT INTO dw.dim_date (date_sk, full_date, year, month_name)
        VALUES (-1, '1900-01-01', 1900, 'Unknown');
        """

        print("🚀 Đang khởi tạo dim_date (2019 - 2030)...")
        cur.execute(sql_script)
        conn.commit()
        print("✅ Xong! Bảng dim_date đã sẵn sàng.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        if conn: cur.close(); conn.close()

if __name__ == "__main__":
    dim_date()