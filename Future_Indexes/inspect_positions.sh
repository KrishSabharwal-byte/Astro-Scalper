/root/future_indexes_3007/venv/bin/python3 << 'EOF'
import sys
sys.path.insert(0, "/root/future_indexes_3007")
from app import client_sessions, get_client_session
from credential_service import credential_service

print("=== ACTIVE CLIENT SESSIONS IN MEMORY ===")
for cid, s in client_sessions.items():
    print(f"Client: {cid}, mode: {s.trading_mode}")
    for slot_id, run in s.run_manager.runs.items():
        open_pos = [p for p in run.positions if p.get("is_open", False)]
        if run.is_running or open_pos:
            print(f"  Slot {slot_id}: is_running={run.is_running}, open_positions={len(open_pos)}, state={run.state}")

print("\n=== CHECKING BROKER OPEN POSITIONS ===")
admin_session = get_client_session("admin")
if admin_session.angel_feed:
    try:
        positions = admin_session.angel_feed.get_positions()
        print("Angel One get_positions result:", positions)
    except Exception as e:
        print("Angel One error:", e)
else:
    print("No angel_feed for admin")

print("\n=== RECENT TRADES IN MONGO FUTURE_INDEXES.admin ===")
from mongo_service import mongo_service
trades = mongo_service.get_recent_trades("admin", limit=10)
for t in trades[:10]:
    print(f"  Trade: {t.get('trade_id')}, run: {t.get('run_id')}, side: {t.get('trade_side')}, qty: {t.get('quantity')}, status: {t.get('status')}, exit_reason: {t.get('exit_reason')}, time: {t.get('exit_time') or t.get('entry_time')}")

EOF
