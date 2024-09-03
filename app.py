import oracledb

DATABASE_URI = {
'user': 'a136861',
'password': 'AbdalrahmanAlsaieda136861',
'dsn': '10.199.104.126/analytics','port':1521
}

try:
    with oracledb.connect(**DATABASE_URI) as conn:
        print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")
