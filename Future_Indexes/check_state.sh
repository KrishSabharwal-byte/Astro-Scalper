/root/future_indexes_3007/venv/bin/python3 << 'EOF'
import json
import urllib.request

data = json.dumps({"client_id": "admin", "password": "123456"}).encode()
req = urllib.request.Request("http://127.0.0.1:3007/auth/login", data=data, headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req) as r:
    res = json.loads(r.read().decode())
    token = res.get("token") or res.get("access_token")

req_state = urllib.request.Request("http://127.0.0.1:3007/state", headers={"Authorization": f"Bearer {token}"})
with urllib.request.urlopen(req_state) as r:
    state = json.loads(r.read().decode())
    runs = state.get("runs", [])
    print("State keys:", list(state.keys()))
    print("Total runs in state:", len(runs))
    for r_data in runs:
        r_id = r_data.get("run_id")
        is_running = r_data.get("is_running")
        pos = r_data.get("positions", [])
        open_pos = [p for p in pos if p.get("is_open", False)]
        trades = r_data.get("trades", [])
        print(f"Slot {r_id}: is_running={is_running}, total_pos={len(pos)}, open_pos={len(open_pos)}, trades={len(trades)}")

EOF
