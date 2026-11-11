import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TEST_DB_NAME = "future_indexes_test_db"
os.environ["MONGO_DB_NAME"] = TEST_DB_NAME

from mongo_service import mongo_service
mongo_service.db_name = TEST_DB_NAME


@pytest.fixture(scope="session", autouse=True)
def isolate_and_cleanup_test_db():
    yield
    # Clean up test database after all tests finish
    if mongo_service.ensure_connection() and mongo_service.client is not None:
        try:
            mongo_service.client.drop_database(TEST_DB_NAME)
        except Exception:
            pass


@pytest.fixture
def test_user_client():
    import uuid
    from fastapi.testclient import TestClient
    from app import app
    from auth_service import auth_service

    client = TestClient(app)
    cid = f"trader_index_{uuid.uuid4().hex[:6]}"
    auth_service.create_user(cid, "Password123!", is_active=True, role="trader")
    res_login = client.post("/auth/login", json={"client_id": cid, "password": "Password123!"})
    assert res_login.status_code == 200
    token = res_login.json()["access_token"]
    client.headers.update({"Authorization": f"Bearer {token}"})
    return client, cid
