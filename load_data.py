import pandas as pd
import sqlalchemy
import time

engine = sqlalchemy.create_engine('mysql+pymysql://dovanthien:210404@localhost:3307/e_commerce_2')

def stream_cleansed_data(csv_path):
    df = pd.read_csv(csv_path)
    
    # Gom nhóm theo order_id để xử lý từng đơn hàng trọn gói
    grouped = df.groupby('order_id')
    
    for order_id, group in grouped:
        # Lấy thông tin chung của đơn hàng từ dòng đầu tiên trong nhóm
        first_row = group.iloc[0]
        
        # 1. Chèn vào bảng orders (Chỉ 1 lần duy nhất cho mỗi đơn hàng)
        order_stmt = sqlalchemy.text("""
            INSERT IGNORE INTO orders (order_id, user_id, status, created_at, num_of_item)
            VALUES (:oid, :uid, :stat, :cat, :num)
        """)
        
        # 2. Chèn vào bảng order_items và UPDATE inventory_items
        # Duyệt qua tất cả các item thuộc đơn hàng này
        with engine.begin() as conn:
            conn.execute(order_stmt, {
                "oid": order_id,
                "uid": first_row['user_id'],
                "stat": first_row['order_status'],
                "cat": first_row['order_created_at'],
                "num": first_row['num_of_item']
            })
            
            for _, item in group.iterrows():
                # Chèn Order Item
                conn.execute(sqlalchemy.text("""
                    INSERT INTO order_items (id, order_id, inventory_item_id, sale_price, created_at, status)
                    VALUES (:id, :oid, :inv_id, :price, :cat, :stat)
                """), {
                    "id": item['order_item_id'],
                    "oid": order_id,
                    "inv_id": item['inventory_item_id'],
                    "price": item['sale_price'],
                    "cat": item['order_created_at'],
                    "stat": item['order_status']
                })
                
                # Cập nhật Inventory (Sự kiện Update cho CDC)
                conn.execute(sqlalchemy.text("""
                    UPDATE inventory_items SET sold_at = :sold WHERE id = :iid
                """), {"sold": item['order_created_at'], "iid": item['inv_id']})
                
        print(f"✅ Đã xử lý xong Đơn hàng: {order_id} ({first_row['num_of_item']} món)")
        time.sleep(2) # Nhỏ giọt mỗi đơn hàng cách nhau 2 giây

stream_cleansed_data('transaction_data.csv')