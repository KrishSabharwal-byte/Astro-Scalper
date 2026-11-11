/root/future_indexes_3007/venv/bin/python3 << 'EOF'
from pymongo import MongoClient

c = MongoClient("mongodb+srv://crestviewcorporate_db_user:Crestviewcorporate@cluster0.zfk4ahy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = c["Future_Indexes"]

for col_name in db.list_collection_names():
    col = db[col_name]
    # Check trades
    count = col.count_documents({"type": "trade"})
    if count > 0:
        recent = list(col.find({"type": "trade"}).sort("entry_time", -1).limit(5))
        latest_time = recent[0].get("entry_time") or recent[0].get("timestamp") or recent[0].get("created_at") if recent else None
        open_trades = list(col.find({"type": "trade", "status": "open"}))
        print(f"Collection '{col_name}': total trades={count}, open_trades={len(open_trades)}, latest trade={latest_time}")
        for t in open_trades:
            print(f"   OPEN: run={t.get('run_id')}, side={t.get('trade_side')}, qty={t.get('quantity')}, fill={t.get('fill_price')}")
        for t in recent[:2]:
            print(f"   RECENT: id={t.get('trade_id')}, run={t.get('run_id')}, status={t.get('status')}, exit_reason={t.get('exit_reason')}, exit_time={t.get('exit_time')}")

EOF
