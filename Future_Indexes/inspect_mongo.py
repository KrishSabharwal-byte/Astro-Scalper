import paramiko
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("187.126.113.7", port=22, username="root", password="Root@#1234567")

remote_script = """import json
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017/')
dbs = client.list_database_names()
print('=' * 60)
print('ALL MONGODB DATABASES ON SERVER: 187.126.113.7:27017')
print('=' * 60)
for db_name in dbs:
    db = client[db_name]
    cols = db.list_collection_names()
    print(f'Database: {db_name}')
    for col_name in cols:
        count = db[col_name].count_documents({})
        print(f'   └── Collection: {col_name} ({count} documents)')
    print()

print('=' * 60)
print('DATABASE IN USE BY THIS APP: FUTURE_INDEXES')
print('=' * 60)
fi_db = client['FUTURE_INDEXES']
for col_name in fi_db.list_collection_names():
    col = fi_db[col_name]
    total = col.count_documents({})
    trades = col.count_documents({'type': 'trade'})
    accounts = col.count_documents({'type': 'user_account'})
    runs_state = col.count_documents({'type': 'runs_state'})
    astro = col.count_documents({'type': 'astro_file'})
    print(f'Collection: FUTURE_INDEXES.{col_name}')
    print(f'   Total: {total} | Trades: {trades} | Accounts: {accounts} | Runs State: {runs_state} | Astro Files: {astro}')
    
    sample_trade = col.find_one({'type': 'trade'}, {'_id': 0})
    if sample_trade:
        print('   Sample Trade Document:')
        print(json.dumps(sample_trade, indent=4))
    print()
"""

sftp = ssh.open_sftp()
with sftp.open("/tmp/inspect_mongo.py", "w") as f:
    f.write(remote_script)
sftp.close()

_, stdout, stderr = ssh.exec_command("/root/future_indexes_3007/venv/bin/python3 /tmp/inspect_mongo.py")
print(stdout.read().decode("utf-8", errors="replace"))
print(stderr.read().decode("utf-8", errors="replace"))

ssh.close()
