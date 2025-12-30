import psycopg2

# DW_CONN_PARAMS = {
#     'host': 'localhost', 'port': 5433, 'database': 'E_COMMERCE',
#     'user': 'dovanthien', 'password': '210404'
# }

DW_CONN_PARAMS = {
    'host': 'postgres', # Tên service trong docker-compose
    'port': 5432,      # Port nội bộ của Docker (không phải 5433)
    'database': 'E_COMMERCE',
    'user': 'dovanthien',
    'password': '210404'
}

def get_dw_conn():
    return psycopg2.connect(**DW_CONN_PARAMS)