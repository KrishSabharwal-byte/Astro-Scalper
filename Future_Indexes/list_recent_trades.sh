/root/future_indexes_3007/venv/bin/python3 << 'EOF'
from pymongo import MongoClient

c = MongoClient("mongodb+srv://crestviewcorporate_db_user:Crestviewcorporate@cluster0.zfk4ahy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = c["Future_Indexes"]
col = db["admin"]

today_trades = list(col.find({"type": "trade"}).sort("entry_time", -1).limit(20))
print(f"Total trades found: {len(today_trades)}")
for t in today_trades:
    entry = t.get("entry_time") or t.get("created_at")
    exit_t = t.get("exit_time")
    print(f"ID: {t.get('trade_id')}, Run: {t.get('run_id')}, Status: {t.get('status')}, Side: {t.get('trade_side')}, Qty: {t.get('quantity')}, Entry: {entry}, Exit: {exit_t}, Reason: {t.get('exit_reason')}, PnL: {t.get('pnl_rupees')}")

EOF
