CREATE TABLE dim_order_status (
    status_key SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL, -- Khóa kinh doanh (Business Key)
    is_final SMALLINT DEFAULT 0  -- Thuộc tính dẫn xuất: 1 nếu trạng thái kết thúc (Delivered/Returned/Cancelled)
);

CREATE TABLE dim_date (
    time_key SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    full_date_string VARCHAR(10) NOT NULL, -- Ví dụ: YYYY-MM-DD
    day_of_week INT NOT NULL,              -- 1 = Thứ Hai, 7 = Chủ Nhật
    day_name VARCHAR(10) NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,           -- Khóa kinh doanh (Business Key)
    age_group VARCHAR(20),                 -- Dẫn xuất từ tuổi thô (Teen, Adult, Senior)
    gender VARCHAR(10),
    city VARCHAR(100),
    country VARCHAR(100),
    traffic_source VARCHAR(50)             -- Nguồn người dùng đến
);


CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE NOT NULL,        -- Khóa kinh doanh (Business Key)
    product_name VARCHAR(255),
    category VARCHAR(100),
    department VARCHAR(100),
    brand VARCHAR(255),
    
    -- Các thuộc tính phân tích khác (bạn có thể thêm nếu muốn giữ nguyên logic ETL trước đó)
    cost DECIMAL(10, 2),
    retail_price DECIMAL(10, 2)
);


CREATE TABLE fact_sales_simple (
    -- Khóa chính của bảng Fact
    fact_key SERIAL PRIMARY KEY, 
    
    -- Các Khóa Ngoại (Foreign Keys)
    user_key INT NOT NULL REFERENCES dim_user(user_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    time_key_order INT NOT NULL REFERENCES dim_date(time_key), -- Ngày đặt hàng
    status_key INT NOT NULL REFERENCES dim_order_status(status_key),
    
    -- Các Phép Đo (Measures)
    quantity INT NOT NULL,                  -- Luôn là 1 trong mô hình này
    sale_price NUMERIC(10, 2) NOT NULL,
    total_revenue NUMERIC(10, 2) NOT NULL,  -- sale_price * quantity
    
    -- Các Cột Mô tả khác (có thể là derived attributes)
    is_returned SMALLINT DEFAULT 0,         -- Cờ: 1 nếu bị trả lại, 0 nếu không

    -- (Tùy chọn) Composite key để tránh trùng lặp nếu chạy ETL nhiều lần
    -- UNIQUE (user_key, product_key, time_key_order, status_key)
);