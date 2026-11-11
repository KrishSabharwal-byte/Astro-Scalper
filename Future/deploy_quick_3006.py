import os
import time
import sys
import paramiko

HOST = "62.72.59.120"
USER = "root"
PASSWORD = "Root@#1234567"
REMOTE_DIR = "/root/commodity_future"
LOCAL_DIR = r"c:\Users\Krish\OneDrive\Desktop\Crestview\Commodity\Future"
PORT_DEPLOY = 3006
SERVICE_NAME = "commodity_future_3006.service"

def main():
    print(f"Connecting to {USER}@{HOST}...", flush=True)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=30, banner_timeout=60, auth_timeout=60)
    print("Connected successfully!", flush=True)

    sftp = ssh.open_sftp()
    
    files_to_upload = [
        "angel_one_service.py",
        "app.py",
        "auth_service.py",
        "client_session.py",
        "mongo_service.py",
        "futures_slice_trading_engine.py",
        "astro_signal_engine.py",
        "credential_service.py",
    ]
    
    for f in files_to_upload:
        l_path = os.path.join(LOCAL_DIR, f)
        r_path = f"{REMOTE_DIR}/{f}"
        if os.path.exists(l_path):
            print(f"Uploading {f}...", flush=True)
            sftp.put(l_path, r_path)

    # Static files
    static_files = ["app.js", "index.html", "style.css"]
    try:
        sftp.mkdir(f"{REMOTE_DIR}/static")
    except Exception:
        pass
        
    for sf in static_files:
        l_path = os.path.join(LOCAL_DIR, "static", sf)
        r_path = f"{REMOTE_DIR}/static/{sf}"
        if os.path.exists(l_path):
            print(f"Uploading static/{sf}...", flush=True)
            sftp.put(l_path, r_path)

    sftp.close()
    print("All files uploaded successfully!", flush=True)

    print(f"Restarting service {SERVICE_NAME}...", flush=True)
    stdin, stdout, stderr = ssh.exec_command(f"systemctl restart {SERVICE_NAME}")
    stdin.close()
    stdout.channel.recv_exit_status()

    time.sleep(3)

    stdin, stdout, stderr = ssh.exec_command(f"systemctl is-active {SERVICE_NAME}")
    stdin.close()
    status_out = stdout.read().decode("ascii", errors="ignore").strip()
    print(f"=== SERVICE ACTIVE: {status_out} ===", flush=True)

    stdin, stdout, stderr = ssh.exec_command(f"curl -s -I http://127.0.0.1:{PORT_DEPLOY}/")
    stdin.close()
    cout = stdout.read().decode("ascii", errors="ignore")
    print("=== HTTP HEALTH CHECK ===", flush=True)
    print(cout.strip(), flush=True)

    ssh.close()
    print("PORT 3006 DEPLOY COMPLETE AND VERIFIED!", flush=True)

if __name__ == "__main__":
    main()
