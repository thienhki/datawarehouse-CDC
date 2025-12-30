# E-commerce Data Warehouse ETL Pipeline

## Mô tả Dự án

Dự án này xây dựng một hệ thống ETL (Extract, Transform, Load) hoàn chỉnh cho kho dữ liệu e-commerce. Hệ thống xử lý dữ liệu từ nguồn MySQL, qua Kafka/Debezium để lưu trữ lịch sử vào Postgres (bronze layer), sau đó biến đổi thành star schema (silver/gold layer) bằng Airflow và Python.

## Kiến trúc Tổng quan

```
[Source: MySQL] → (Ingestion: Debezium + Kafka) → [Bronze Layer: Postgres Staging] (Lưu lịch sử: op, ts_ms)
                                                                 ↓
                                                    (Processing: Airflow + Python ETL)
                                                                 ↓
                                    [Silver/Gold Layer: Postgres Star Schema] (Fact & Dimensions)
```

### Các Thành phần Chính:
- **MySQL**: Nguồn dữ liệu OLTP
- **Debezium + Kafka**: CDC (Change Data Capture) để capture thay đổi từ MySQL
- **Postgres Bronze**: Lưu trữ dữ liệu thô với lịch sử (op: operation, ts_ms: timestamp)
- **Airflow**: Orchestration cho ETL pipeline
- **Postgres Silver/Gold**: Star schema với dimensions (user, product, dc_center, date, status) và fact_sales

## Yêu cầu Tiên quyết

- Docker & Docker Compose
- Python 3.10+
- Git

## Cài đặt và Thiết lập

### 1. Clone Repository
```bash
git clone https://github.com/thienhki/datawarehouse.git
cd datawarehouse
```

### 2. Khởi động Services
```bash
docker-compose up -d
```

Services sẽ khởi động:
- Zookeeper & Kafka
- Debezium Connect
- MySQL (port 3307)
- Postgres (port 5433)
- Airflow Webserver (port 8080) & Scheduler

### 3. Cài đặt Dependencies
```bash
pip install -r requirements.txt
```

### 4. Load Dữ liệu Ban đầu vào MySQL
```bash
python load_data.py
```
Script này sẽ load dữ liệu từ `transaction_data.csv` vào MySQL database `e_commerce`.

### 5. Thiết lập Kafka Connectors

#### Source Connector (MySQL → Kafka)
```bash
curl -X POST -H "Content-Type: application/json" \
--data @source_connector/mysql-ecommerce-source.json \
http://localhost:8083/connectors
```

#### Sink Connectors (Kafka → Postgres Bronze)
```bash
for file in sink_connector/*.json; do
  echo "Creating connector from: $file"
  curl -X POST -H "Content-Type: application/json" \
  --data @"$file" \
  http://localhost:8083/connectors
done
```

## Cách Sử dụng

### 1. Giám sát Connectors
```bash
# Kiểm tra trạng thái connectors
curl http://localhost:8083/connectors | jq

# Kiểm tra trạng thái cụ thể
curl http://localhost:8083/connectors/postgres-orders-sink/status | jq
```

### 2. Xem Kafka Topics
```bash
# Danh sách topics
docker exec -it kafka_demo kafka-topics --bootstrap-server kafka:9092 --list

# Xem dữ liệu streaming
docker exec -it kafka_demo kafka-console-consumer \
--bootstrap-server kafka:9092 \
--topic mysql_ecommerce_server.orders \
--from-beginning
```

### 3. Chạy ETL Pipeline
- Truy cập Airflow UI: http://localhost:8080
- Tìm DAG `e_commerce_full_pipeline`
- Trigger DAG để chạy ETL từ bronze → silver/gold

DAG sẽ:
- Transform dimensions: user, product, dc_center
- Transform fact: sales
- Sử dụng checkpoints để tránh duplicate data

## Cấu hình

### Database Connections
- MySQL: `mysql+pymysql://dovanthien:210404@localhost:3307/e_commerce`
- Postgres: `postgresql://dovanthien:210404@localhost:5433/E_COMMERCE`

### Connectors
- Source: `source_connector/mysql-ecommerce-source.json`
- Sinks: `sink_connector/*.json`

### Checkpoints
- Lưu trữ trong `checkpoints/` để track tiến độ ETL
- Files: `users.json`, `products.json`, `dc.json`, `fact_sales.json`

## Xử lý Sự cố

### Connectors không hoạt động
```bash
# Xóa và tạo lại connector
curl -X DELETE http://localhost:8083/connectors/mysql-ecommerce-source
# Sau đó tạo lại
```

### Dữ liệu không đồng bộ
- Kiểm tra logs: `docker logs debezium_demo`
- Restart services: `docker-compose restart`

### Airflow không kết nối được
- Đảm bảo Postgres đang chạy
- Kiểm tra connection string trong `docker-compose.yaml`

## Cấu trúc Thư mục

```
├── dags/                          # Airflow DAGs
│   └── e_commerce_etl.py
├── data_sources/                  # Dữ liệu nguồn CSV
├── checkpoints/                   # Checkpoint files
├── config/                        # Database configs
├── connect-plugins/               # Debezium plugins
├── load_dataset_into_mysql/       # SQL scripts
├── sink_connector/                # Kafka sink configs
├── source_connector/              # Kafka source configs
├── docker-compose.yaml            # Services definition
├── load_data.py                   # Initial data loader
└── requirements.txt               # Python dependencies
```

## Phát triển và Đóng góp

- Thêm transforms mới trong `dags/etl/transform/`
- Update DAG dependencies trong `e_commerce_etl.py`
- Test locally trước khi commit

## License

[Thêm license nếu có]
