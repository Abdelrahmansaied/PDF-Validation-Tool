import oracledb

DATABASE_URI = {
    'user': 'your_user',
    'password': 'your_password',
    'dsn': 'your_dsn',
    'port': 1521
}

try:
    with oracledb.connect(**DATABASE_URI) as conn:
        print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")
