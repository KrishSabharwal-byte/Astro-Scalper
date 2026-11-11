"""
deploy.py  —  SSH deployment for Future_Indexes FastAPI app.
Uses paramiko (already installed). Runs all remote steps individually.
"""

import io
import os
import sys
import zipfile
import paramiko

# Force UTF-8 on Windows consoles
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Config ──────────────────────────────────────────────────────
HOST       = "187.126.113.7"
SSH_PORT   = 22
USER       = "root"
PASSWORD   = "Root@#1234567"
APP_PORT   = 3007
REMOTE_DIR   = "/root/future_indexes_3007"
SERVICE_NAME = "future_indexes_3007"
ZIP_TMP      = "/tmp/future_indexes.zip"

EXCLUDE_DIRS  = {"__pycache__", ".pytest_cache", "logs", ".git", ".agents"}
EXCLUDE_FILES = {"deploy.py", "check_remote.py"}
PROJECT_ROOT  = os.path.dirname(os.path.abspath(__file__))

# ── Helpers ──────────────────────────────────────────────────────

def log(msg, tag="INFO"):
    tags = {"INFO": "[INFO]", "OK": "[OK]  ", "WARN": "[WARN]", "ERR": "[ERR] "}
    print(f"{tags.get(tag, tag)} {msg}", flush=True)


def build_zip():
    buf = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
            rel_root = os.path.relpath(root, PROJECT_ROOT)
            for fname in files:
                if fname in EXCLUDE_FILES or fname.endswith(".pyc"):
                    continue
                local = os.path.join(root, fname)
                arc   = fname if rel_root == "." else os.path.join(rel_root, fname)
                zf.write(local, arc)
                count += 1
    log(f"Packed {count} files  ({len(buf.getvalue()) // 1024} KB)")
    buf.seek(0)
    return buf.getvalue()


def run(ssh, cmd, check=True):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=600)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    rc  = stdout.channel.recv_exit_status()
    combined = (out + err).strip()
    for line in combined.splitlines():
        log(f"  {line}")
    if check and rc != 0:
        log(f"FAILED (exit {rc}): {cmd}", "ERR")
        sys.exit(1)
    return combined, rc


def connect(host, port):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port=port, username=USER, password=PASSWORD,
                timeout=30, allow_agent=False, look_for_keys=False)
    return ssh


# ── Main ─────────────────────────────────────────────────────────

def main():
    log("=" * 58)
    log(f" Future Indexes — SSH Deployment")
    log(f" Target : root@{HOST}  ->  port {APP_PORT}")
    log(f" Remote : {REMOTE_DIR}")
    log("=" * 58)

    # 1. Build ZIP
    log("Step 1 — Building ZIP...")
    zip_data = build_zip()

    # 2. SSH connect
    log("Step 2 — Connecting via SSH...")
    ssh = None
    for port in [SSH_PORT, 3007]:
        try:
            ssh = connect(HOST, port)
            log(f"Connected on port {port}", "OK")
            break
        except Exception as e:
            log(f"Port {port} failed: {e}", "WARN")
    if ssh is None:
        log("Could not connect to server.", "ERR")
        sys.exit(1)

    # 3. Upload ZIP via SFTP
    log("Step 3 — Uploading ZIP via SFTP...")
    sftp = ssh.open_sftp()
    buf = io.BytesIO(zip_data)
    sftp.putfo(buf, ZIP_TMP, file_size=len(zip_data))
    sftp.close()
    log(f"Uploaded -> {ZIP_TMP}", "OK")

    # 4. Remote setup
    log("Step 4 — Remote setup...")

    # 4a. Ensure app directory exists
    log("  [4a] Creating app directory...")
    run(ssh, f"mkdir -p {REMOTE_DIR}")

    # 4b. Stop any conflicting legacy service
    log("  [4b] Cleaning up temporary services...")
    run(ssh, "systemctl stop --no-block future_indexes 2>/dev/null || true", check=False)
    run(ssh, "systemctl disable future_indexes 2>/dev/null || true", check=False)
    run(ssh, f"systemctl stop --no-block {SERVICE_NAME} 2>/dev/null || true", check=False)
    run(ssh, f"pkill -9 -f 'uvicorn.*{APP_PORT}' 2>/dev/null || true", check=False)
    run(ssh, "sleep 1", check=False)

    # 4c. Extract ZIP
    log("  [4c] Extracting application...")
    extract_cmd = (
        f"python3 -c \""
        f"import zipfile; "
        f"zf=zipfile.ZipFile('{ZIP_TMP}'); "
        f"zf.extractall('{REMOTE_DIR}'); "
        f"print('Extracted', len(zf.namelist()), 'files to {REMOTE_DIR}')\""
    )
    run(ssh, extract_cmd)

    # 4d. Ensure virtualenv exists
    log("  [4d] Checking / creating virtualenv...")
    run(ssh, f"test -f {REMOTE_DIR}/venv/bin/python || python3 -m venv {REMOTE_DIR}/venv")

    # 4e. Install requirements
    log("  [4e] Installing requirements...")
    run(ssh, f"{REMOTE_DIR}/venv/bin/pip install -r {REMOTE_DIR}/requirements.txt -q")

    # 4f. Configure .env with MongoDB Atlas connection string
    log("  [4f] Configuring .env with MongoDB Atlas...")
    env_content = f"""PORT={APP_PORT}
HOST=0.0.0.0
MONGO_URI=mongodb+srv://crestviewcorporate_db_user:Crestviewcorporate@cluster0.zfk4ahy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
MONGO_DB_NAME=Future_Indexes
ALLOW_INSECURE_DEV_AUTH=1
AUTH_SECRET_KEY=dev_slicer_master_encryption_key_2026
ENCRYPTION_MASTER_KEY=dev_slicer_master_encryption_key_2026
"""
    try:
        sftp_env = ssh.open_sftp()
        with sftp_env.open(f"{REMOTE_DIR}/.env", "w") as f:
            f.write(env_content)
        sftp_env.close()
        log("Configured .env with MongoDB Atlas Cluster0", "OK")
    except Exception as e:
        log(f"Error configuring .env: {e}", "WARN")

    # 5. Install & start systemd service
    log(f"Step 5 — Setting up {SERVICE_NAME}.service...")
    service_content = f"""[Unit]
Description=Future Indexes Scalper Service Port {APP_PORT}
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={REMOTE_DIR}
EnvironmentFile={REMOTE_DIR}/.env
ExecStart={REMOTE_DIR}/venv/bin/uvicorn app:app --host 0.0.0.0 --port {APP_PORT} --workers 1
Restart=always
RestartSec=3s
StandardOutput=append:{REMOTE_DIR}/uvicorn.log
StandardError=append:{REMOTE_DIR}/uvicorn.log
KillMode=process

[Install]
WantedBy=multi-user.target
"""
    try:
        sftp2 = ssh.open_sftp()
        with sftp2.open(f"/etc/systemd/system/{SERVICE_NAME}.service", "w") as f:
            f.write(service_content)
        sftp2.close()
        run(ssh, "systemctl daemon-reload")
        run(ssh, f"systemctl enable {SERVICE_NAME}")
        run(ssh, f"systemctl restart --no-block {SERVICE_NAME}")
        log(f"{SERVICE_NAME}.service configured and restarted.", "OK")
    except Exception as e:
        log(f"Systemd error: {e}", "WARN")

    # 6. Verify health
    log("Step 6 — Verifying service status...")
    run(ssh, "sleep 3", check=False)
    run(ssh, f"systemctl status {SERVICE_NAME} --no-pager -l || true", check=False)
    run(ssh, f"tail -n 25 {REMOTE_DIR}/uvicorn.log || true", check=False)
    run(ssh, f"curl -s -i http://127.0.0.1:{APP_PORT}/ || true", check=False)

    ssh.close()

    log("=" * 58, "OK")
    log(" DEPLOYMENT COMPLETE!", "OK")
    log(f" App URL : http://{HOST}:{APP_PORT}/", "OK")
    log(f" Logs    : ssh root@{HOST} 'tail -f {REMOTE_DIR}/uvicorn.log'", "OK")
    log("=" * 58, "OK")


if __name__ == "__main__":
    main()

