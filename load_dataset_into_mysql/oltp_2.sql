-- 1. Bảng Trung tâm phân phối
CREATE TABLE distribution_centers (
    id         INT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    latitude   DECIMAL(10, 8),
    longitude  DECIMAL(11, 8)
);

-- 2. Bảng Sản phẩm (Chỉ lưu thông tin gốc của sản phẩm)
CREATE TABLE products (
    id                  INT PRIMARY KEY,
    cost                DECIMAL(10,2),
    category            VARCHAR(100),
    name                VARCHAR(255),
    brand               VARCHAR(255),
    retail_price        DECIMAL(10,2), -- Giá niêm yết hiện tại
    department          VARCHAR(100),
    sku                 VARCHAR(50) UNIQUE,
    distribution_center_id INT,
    FOREIGN KEY (distribution_center_id) REFERENCES distribution_centers(id)
);

-- 3. Bảng Người dùng
CREATE TABLE users (
    id              INT PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255) UNIQUE,
    age             INT,
    gender          VARCHAR(10),
    state           VARCHAR(50),
    street_address  VARCHAR(255),
    postal_code     VARCHAR(20),
    city            VARCHAR(100),
    country         VARCHAR(100),
    latitude        DECIMAL(11,8),
    longitude       DECIMAL(11,8),
    traffic_source  VARCHAR(50),
    created_at      DATETIME 
);

-- 4. Bảng Mặt hàng tồn kho (Đã loại bỏ dư thừa sản phẩm)
CREATE TABLE inventory_items (
    id                          INT PRIMARY KEY,
    product_id                  INT NOT NULL,
    created_at                  DATETIME DEFAULT CURRENT_TIMESTAMP,
    sold_at                     DATETIME NULL,
    cost                        DECIMAL(10,2), -- Giá vốn tại thời điểm nhập kho
    distribution_center_id      INT,
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (distribution_center_id) REFERENCES distribution_centers(id)
);

-- 5. Bảng Đơn hàng (Loại bỏ cột gender dư thừa)
CREATE TABLE orders (
    order_id       INT PRIMARY KEY,
    user_id        INT NOT NULL,
    status         VARCHAR(20),
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    returned_at    DATETIME NULL,
    shipped_at     DATETIME NULL,
    delivered_at   DATETIME NULL,
    num_of_item    INT DEFAULT 1,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- 6. Bảng Chi tiết đơn hàng (Lưu giá thực tế lúc mua)
CREATE TABLE order_items (
    id                  INT PRIMARY KEY,
    order_id            INT NOT NULL,
    inventory_item_id   INT NOT NULL, -- Từ đây có thể suy ra product_id qua bảng inventory_items
    status              VARCHAR(20) NOT NULL,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    shipped_at          DATETIME NULL,
    delivered_at        DATETIME NULL,
    returned_at         DATETIME NULL,
    sale_price          DECIMAL(10,2) NOT NULL, -- Giá bán thực tế tại thời điểm mua
    FOREIGN KEY (order_id)          REFERENCES orders(order_id),
    FOREIGN KEY (inventory_item_id) REFERENCES inventory_items(id)
);

-- 7. Bảng Sự kiện (Đã loại bỏ thông tin địa lý dư thừa)
CREATE TABLE events (
    id              BIGINT PRIMARY KEY,
    user_id         INT,
    session_id      VARCHAR(255),
    sequence_number INT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    ip_address      VARCHAR(45),
    browser         VARCHAR(50),
    traffic_source  VARCHAR(50),
    uri             VARCHAR(500),
    event_type      VARCHAR(100),
    FOREIGN KEY (user_id) REFERENCES users(id)


