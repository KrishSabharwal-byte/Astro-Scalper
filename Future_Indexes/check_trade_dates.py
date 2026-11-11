import paramiko
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("187.126.113.7", port=22, username="root", password="Root@#1234567")

script = """
import json
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017/')
db = client['FUTURE_INDEXES']

for col_name in ['admin', 'User1', 'MKT11060']:
    col = db[col_name]
    cursor = col.find({'type': 'trade'}).sort('_id', 1)
    trades = list(cursor)
    print(f'=== Collection FUTURE_INDEXES.{col_name} (Total trades: {len(trades)}) ===')
    if trades:
        first = trades[0]
        last = trades[-1]
        print(f'First trade: id={first.get("trade_id")} created_at={first.get("created_at")} exit_time={first.get("exit_time")} contract={first.get("contract_symbol")} inst={first.get("instrument")}')
        print(f'Last trade:  id={last.get("trade_id")} created_at={last.get("created_at")} exit_time={last.get("exit_time")} contract={last.get("contract_symbol")} inst={last.get("instrument")}')
        
        # Count by date
        date_counts = {}
        for t in trades:
            d = (t.get("created_at") or t.get("exit_time") or "")[:10]
            date_counts[d] = date_counts.get(d, 0) + 1
        print('Trade counts by date:')
        for d, count in sorted(date_counts.items()):
            print(f'   {d}: {count} trades')
    print()
"""

sftp = ssh.open_sftp()
with sftp.open("/tmp/check_trade_dates.py", "w") as f:
    f.write(script)
sftp.close()

_, stdout, stderr = ssh.exec_command("/root/future_indexes_3007/venv/bin/python3 /tmp/check_trade_dates.py")
print(stdout.read().decode("utf-8", errors="replace"))
print(stderr.read().decode("utf-8", errors="replace"))

ssh.close()
