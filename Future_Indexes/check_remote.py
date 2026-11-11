import paramiko
import sys

# Force UTF-8 on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("187.126.113.7", port=22, username="root", password="Root@#1234567")

def run(cmd):
    print(f"\n>>> {cmd}")
    _, o, e = ssh.exec_command(cmd)
    out = o.read().decode("utf-8", errors="replace")
    err = e.read().decode("utf-8", errors="replace")
    print(out + err)

run("systemctl status mongod --no-pager || systemctl status mongodb --no-pager")
run("/root/future_indexes_3007/venv/bin/python3 -c \"from pymongo import MongoClient; c=MongoClient('mongodb://127.0.0.1:27017/'); print('Databases:', c.list_database_names()); db=c['Future_Indexes']; print('Collections in Future_Indexes:', db.list_collection_names()); print('admin docs count:', db['admin'].count_documents({})); print('admin trades count:', db['admin'].count_documents({'type': 'trade'}))\"")
run("grep -i 'mongo' /root/future_indexes_3007/uvicorn.log | tail -n 20")
run("tail -n 30 /root/future_indexes_3007/uvicorn.log")

ssh.close()
