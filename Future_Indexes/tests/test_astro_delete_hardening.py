import pytest
import io
from fastapi.testclient import TestClient
from mongo_service import mongo_service
from app import app, get_client_session


@pytest.fixture
def api_client():
    return TestClient(app)


def test_delete_active_file_does_not_auto_promote_historical_files(test_user_client):
    """
    Verification 1 & 4: Upload File A then File B (B is active).
    Delete B. Confirm get_active_astro_file returns None (File A not promoted).
    Confirm GET /state shows astro_state.has_active_file == False.
    """
    client, cid = test_user_client
    session = get_client_session(cid)

    csv_a = (
        "Date,Time,Direction\n"
        "2026-02-01,10:00,Upside\n"
        "2026-02-01,10:10,Upside\n"
        "2026-02-01,10:20,Upside\n"
    )
    csv_b = (
        "Date,Time,Direction\n"
        "2026-02-01,11:00,Downside\n"
        "2026-02-01,11:10,Downside\n"
        "2026-02-01,11:20,Downside\n"
    )

    # 1. Upload File A
    res_a = client.post(
        "/astro/upload",
        files={"file": ("file_A.csv", io.BytesIO(csv_a.encode("utf-8")), "text/csv")},
    )
    assert res_a.status_code == 200
    fid_a = res_a.json()["file_id"]

    # 2. Upload File B (becomes active)
    res_b = client.post(
        "/astro/upload",
        files={"file": ("file_B.csv", io.BytesIO(csv_b.encode("utf-8")), "text/csv")},
    )
    assert res_b.status_code == 200
    fid_b = res_b.json()["file_id"]

    # Verify B is active
    active = mongo_service.get_active_astro_file(client_id=cid)
    assert active is not None
    assert active["filename"] == "file_B.csv"

    # 3. Delete active File B
    res_del = client.delete(f"/astro/files/{fid_b}")
    assert res_del.status_code == 200
    assert res_del.json()["status"] == "success"
    assert res_del.json()["deleted"] is True
    # Evaluation on delete must be NO_FILE
    assert res_del.json()["evaluation"]["status"] == "NO_FILE"

    # Verify get_active_astro_file returns None (File A was NOT silently auto-promoted)
    active_after_del = mongo_service.get_active_astro_file(client_id=cid)
    assert active_after_del is None

    # Verify cache also returns None
    cached_active = mongo_service.get_active_astro_file(client_id=cid, use_cache=True)
    assert cached_active is None

    # 4. Confirm GET /state shows has_active_file: False
    res_state = client.get("/state")
    assert res_state.status_code == 200
    state_payload = res_state.json()
    astro_state = state_payload["astro_state"]
    assert astro_state["has_active_file"] is False
    assert astro_state["filename"] is None
    assert astro_state["file_id"] is None
    assert astro_state["cluster_preview"] is None
    assert astro_state["active_signal"] == "NEUTRAL"

    # Also confirm GET /astro/state
    res_astro_state = client.get("/astro/state")
    assert res_astro_state.status_code == 200
    assert res_astro_state.json()["astro_state"]["has_active_file"] is False

    # 5. Confirm File A still exists in upload history and can be activated explicitly
    res_files = client.get("/astro/files")
    assert res_files.status_code == 200
    history = res_files.json()["files"]
    assert any(f["file_id"] == fid_a for f in history)
    assert not any(f["file_id"] == fid_b for f in history)

    # Reactivate File A explicitly
    res_act = client.post(f"/astro/activate/{fid_a}")
    assert res_act.status_code == 200
    assert res_act.json()["status"] == "success"

    active_reactivated = mongo_service.get_active_astro_file(client_id=cid)
    assert active_reactivated is not None
    assert active_reactivated["filename"] == "file_A.csv"

    # Now delete File A, ensuring client is left with no active file
    res_del_a = client.delete(f"/astro/files/{fid_a}")
    assert res_del_a.status_code == 200
    assert mongo_service.get_active_astro_file(client_id=cid) is None


@pytest.mark.asyncio
async def test_evaluate_fixed_slots_returns_no_file_after_delete(test_user_client):
    """
    Verification 2: Confirm evaluate_fixed_slots() returns status: 'NO_FILE'
    right after delete, and repeated calls do not produce signals.
    """
    client, cid = test_user_client
    session = get_client_session(cid)

    csv_data = (
        "Date,Time,Direction\n"
        "2026-02-01,10:00,Upside\n"
        "2026-02-01,10:10,Upside\n"
        "2026-02-01,10:20,Upside\n"
    )

    fid = mongo_service.save_astro_file(
        client_id=cid,
        filename="test_cluster.csv",
        content=csv_data,
        row_count=3,
        is_active=True,
    )

    # Evaluate before delete (file is present, status is not NO_FILE)
    eval_before = await session.evaluate_fixed_slots()
    assert eval_before["status"] != "NO_FILE"

    # Delete the active file
    mongo_service.delete_astro_file(client_id=cid, file_id=fid)

    # Evaluate after delete
    eval_after = await session.evaluate_fixed_slots()
    assert eval_after["status"] == "NO_FILE"
    assert eval_after["signal"] == "NEUTRAL"
    assert session.last_astro_preview is None

    # Repeated calls also remain NO_FILE
    eval_repeat = await session.evaluate_fixed_slots()
    assert eval_repeat["status"] == "NO_FILE"
    assert eval_repeat["signal"] == "NEUTRAL"


def test_dual_storage_consistency_and_memory_fallback():
    """
    Verification 5 & Requirement 8: Audit dual-storage path.
    Assert that MongoDB collection and in-memory store do not diverge,
    and that memory fallback does not resurrect deleted files.
    """
    test_cid = "test_dual_storage_client"
    # Clean any leftover in memory or DB
    mongo_service._in_memory_astro_files.pop(test_cid, None)
    if hasattr(mongo_service, "_active_file_cache"):
        mongo_service._active_file_cache.pop(test_cid, None)

    csv_content = "Date,Time,Direction\n2026-02-01,10:00,Upside\n"
    fid1 = mongo_service.save_astro_file(
        client_id=test_cid,
        filename="report_1.csv",
        content=csv_content,
        row_count=1,
        is_active=True,
    )
    fid2 = mongo_service.save_astro_file(
        client_id=test_cid,
        filename="report_2.csv",
        content=csv_content,
        row_count=1,
        is_active=False,
    )

    # Ensure in-memory list has both files and fid1 is active
    mem_files = mongo_service._in_memory_astro_files.get(test_cid, [])
    assert len(mem_files) == 2
    f1_mem = next(f for f in mem_files if str(f.get("_id")) == str(fid1))
    f2_mem = next(f for f in mem_files if str(f.get("_id")) == str(fid2))
    assert f1_mem["active"] is True
    assert f2_mem["active"] is False

    # Delete active file fid1
    mongo_service.delete_astro_file(client_id=test_cid, file_id=fid1)

    # In-memory must have only 1 file, and its active status must NOT be True
    assert len(mongo_service._in_memory_astro_files.get(test_cid, [])) == 1
    remaining_mem = mongo_service._in_memory_astro_files[test_cid][0]
    assert str(remaining_mem.get("_id")) == str(fid2)
    assert remaining_mem.get("active") is False  # Must NOT be auto-promoted!

    # Active file must be None
    assert mongo_service.get_active_astro_file(client_id=test_cid) is None

    # Delete the second file as well
    mongo_service.delete_astro_file(client_id=test_cid, file_id=fid2)
    assert len(mongo_service.list_astro_files(client_id=test_cid)) == 0
    assert mongo_service.get_active_astro_file(client_id=test_cid) is None
