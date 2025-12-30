import pandas as pd

distribution_centers = pd.read_csv('./data_do_an/distribution_centers.csv')

# print(distribution_centers.head())
# print(distribution_centers.info())
# print(distribution_centers.isnull().sum())
# print(distribution_centers.duplicated().sum())
# events = pd.read_csv('./data_do_an/events.csv')
# print(events.head())
# print(events.info())
# print(events.isnull().sum())
# print(events.duplicated().sum())

inventory = pd.read_csv('./data_do_an/inventory_items.csv')
# print(inventory.head())
# print(inventory.info())
# print(inventory.isnull().sum())
# print(inventory.duplicated().sum())
# print(inventory[inventory['product_name'].isnull()][['product_name', 'product_brand']])

order_items = pd.read_csv('./data_do_an/order_items.csv')
# print(order_items.head())
# print(order_items.info())
# print(order_items.isnull().sum())
# print(order_items.duplicated().sum())

# orders = pd.read_csv('./data_do_an/orders.csv')
# print(orders.head())
# print(orders.info())
# print(orders.isnull().sum())
# print(orders.duplicated().sum())


products = pd.read_csv('./data_do_an/products.csv')
# print(products.head())
# print(products.info())
# print(products.isnull().sum())
# print(products.duplicated().sum())

users = pd.read_csv('./data_sources/users.csv')
# print(users.head())
# print(users.info())
# print(users.isnull().sum())
# print(users.duplicated().sum())

# users['latitude'] = users['latitude'].astype(str).str.strip().astype(float)
# users['longitude'] = users['longitude'].astype(str).str.strip().astype(float)

# users.to_csv('./do_an_source/users_clean.csv', index=False)

# print(len(users))

# LOAD DATA LOCAL INFILE '/tmp/data/users_clean.csv'
# INTO TABLE users
# FIELDS TERMINATED BY ',' 
# ENCLOSED BY '"'
# LINES TERMINATED BY '\n'
# IGNORE 1 ROWS
# (id, first_name, last_name, email, age, gender, state, street_address, postal_code, city, country,
#  latitude, longitude, traffic_source, created_at);
# print(users[users['id'] == 41])
orders = pd.read_csv('./data_sources/orders.csv')

print(len(order_items))




