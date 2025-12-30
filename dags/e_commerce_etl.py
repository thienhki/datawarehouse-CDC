from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 1. Khai báo đường dẫn để Airflow tìm thấy module 'etl'
# Giả sử dự án của bạn nằm tại /home/dovanthien/do_an_etl


# 2. Import các công cụ và hàm transform đã viết
from etl.common.database import get_dw_conn
from etl.common.checkpoint import CheckpointManager
from etl.transform.transform_dim_user import etl_dim_user
from etl.transform.transform_dim_product import etl_dim_product
from etl.transform.transform_dim_dc_center import etl_dim_dc
from etl.transform.transform_fact_sales import etl_fact_sales

# 3. Hàm wrapper để quản lý Transaction cho mỗi Task
def run_etl_step(etl_func, checkpoint_name):
    conn = get_dw_conn()
    try:
        # Gọi hàm logic từ folder transform
        # Các hàm này trả về: (max_ts, count_inserted, [count_closed])
        result = etl_func(conn)
        max_ts = result[0]
        
        # Lưu thay đổi vào Database
        conn.commit()
        
        # Nếu nạp thành công, cập nhật checkpoint
        if max_ts is not None:
            CheckpointManager().save(checkpoint_name, max_ts)

            
        print(f"✅ Task {checkpoint_name} finished successfully.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Task {checkpoint_name} failed. Rolling back...")
        raise e
    finally:
        conn.close()

# 4. Định nghĩa DAG
default_args = {
    'owner': 'dovanthien',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='e_commerce_full_pipeline',
    default_args=default_args,
    description='ETL pipeline for E-commerce Data Warehouse',
    schedule_interval='@hourly', # Chạy mỗi giờ một lần
    catchup=False
) as dag:

    # Khai báo các Task
    task_user = PythonOperator(
        task_id='etl_dim_user',
        python_callable=run_etl_step,
        op_args=[etl_dim_user, "users"]
    )

    task_product = PythonOperator(
        task_id='etl_dim_product',
        python_callable=run_etl_step,
        op_args=[etl_dim_product, "products"]
    )

    task_dc = PythonOperator(
        task_id='etl_dim_dc',
        python_callable=run_etl_step,
        op_args=[etl_dim_dc, "dc"]
    )

    task_fact = PythonOperator(
        task_id='etl_fact_sales',
        python_callable=run_etl_step,
        op_args=[etl_fact_sales, "fact_sales"]
    )

    # 5. Thiết lập luồng chạy (Dependencies)
    # Chạy 3 bảng Dim song song, sau đó mới chạy Fact
    [task_user, task_product, task_dc] >> task_fact