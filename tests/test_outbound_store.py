import pytest


@pytest.mark.asyncio
async def test_outbound_store_campaign_import_and_leasing(tmp_path, monkeypatch):
    monkeypatch.setenv("CALL_HISTORY_ENABLED", "true")
    db_path = str(tmp_path / "call_history.db")

    from src.core.outbound_store import OutboundStore

    store = OutboundStore(db_path=db_path)

    campaign = await store.create_campaign(
        {
            "name": "Test Campaign",
            "timezone": "UTC",
            "daily_window_start_local": "09:00",
            "daily_window_end_local": "17:00",
            "max_concurrent": 1,
            "min_interval_seconds_between_calls": 0,
            "default_context": "demo",
            "voicemail_drop_mode": "upload",
            "voicemail_drop_media_uri": "sound:ai-generated/test-vm",
        }
    )
    campaign_id = campaign["id"]

    csv_bytes = (
        "phone_number,custom_vars,context,timezone\n"
        '+15551230001,"{""name"":""Alice""}",demo,UTC\n'
        '+15551230001,"{""name"":""Alice""}",demo,UTC\n'
        '+15551230002,"{""name"":""Bob""}",demo,UTC\n'
    ).encode("utf-8")

    imported = await store.import_leads_csv(campaign_id, csv_bytes, skip_existing=True, max_error_rows=20)
    assert imported["accepted"] == 2
    assert imported["duplicates"] == 1
    assert imported["rejected"] == 0

    leads_page = await store.list_leads(campaign_id, page=1, page_size=50)
    assert leads_page["total"] == 2
    lead_ids = {l["id"] for l in leads_page["leads"]}
    assert len(lead_ids) == 2

    leased = await store.lease_pending_leads(campaign_id, limit=1, lease_seconds=60)
    assert len(leased) == 1
    lead = leased[0]
    assert lead["state"] == "leased"
    assert lead["phone_number"].startswith("+1555")
    assert isinstance(lead.get("custom_vars"), dict)

    marked = await store.mark_lead_dialing(lead["id"])
    assert marked is True

    # Second mark should fail (not leased anymore)
    marked2 = await store.mark_lead_dialing(lead["id"])
    assert marked2 is False

    await store.set_lead_state(lead["id"], state="completed", last_outcome="answered_human")

    # Leasing again should pick the other pending lead.
    leased2 = await store.lease_pending_leads(campaign_id, limit=1, lease_seconds=60)
    assert len(leased2) == 1
    assert leased2[0]["id"] != lead["id"]


@pytest.mark.asyncio
async def test_extra_columns_become_custom_vars(tmp_path, monkeypatch):
    """Extra CSV columns (not in the reserved set) must be auto-promoted to custom_vars."""
    monkeypatch.setenv("CALL_HISTORY_ENABLED", "true")
    db_path = str(tmp_path / "extra_cols.db")

    from src.core.outbound_store import OutboundStore

    store = OutboundStore(db_path=db_path)

    campaign = await store.create_campaign(
        {
            "name": "Extra Col Campaign",
            "timezone": "UTC",
            "daily_window_start_local": "09:00",
            "daily_window_end_local": "17:00",
            "max_concurrent": 1,
            "min_interval_seconds_between_calls": 0,
            "default_context": "demo",
            "voicemail_drop_mode": "upload",
            "voicemail_drop_media_uri": "sound:ai-generated/test-vm",
        }
    )
    campaign_id = campaign["id"]

    # CSV with plain extra columns (no JSON custom_vars column needed)
    csv_bytes = (
        "name,phone_number,context,timezone,first_name,account_id\n"
        "Alice Example,+15557001001,demo,UTC,Alice,ACC-001\n"
        "Bob Example,+15557001002,demo,UTC,Bob,ACC-002\n"
    ).encode("utf-8")

    imported = await store.import_leads_csv(campaign_id, csv_bytes, skip_existing=True, max_error_rows=20)
    assert imported["accepted"] == 2
    assert imported["rejected"] == 0

    leased = await store.lease_pending_leads(campaign_id, limit=2, lease_seconds=60)
    assert len(leased) == 2

    by_phone = {l["phone_number"]: l for l in leased}
    alice = by_phone["+15557001001"]

    # Extra columns must appear in custom_vars
    assert alice["custom_vars"]["first_name"] == "Alice"
    assert alice["custom_vars"]["account_id"] == "ACC-001"

    # Standard lead fields must also be seeded
    assert alice["custom_vars"]["lead_name"] == "Alice Example"
    assert alice["custom_vars"]["phone_number"] == "+15557001001"
    assert alice["custom_vars"]["lead_timezone"] == "UTC"

    bob = by_phone["+15557001002"]
    assert bob["custom_vars"]["first_name"] == "Bob"
    assert bob["custom_vars"]["account_id"] == "ACC-002"


@pytest.mark.asyncio
async def test_json_custom_vars_wins_over_extra_column(tmp_path, monkeypatch):
    """When a key appears in both the JSON custom_vars column and a plain extra column,
    the JSON value must take priority."""
    monkeypatch.setenv("CALL_HISTORY_ENABLED", "true")
    db_path = str(tmp_path / "conflict.db")

    from src.core.outbound_store import OutboundStore

    store = OutboundStore(db_path=db_path)

    campaign = await store.create_campaign(
        {
            "name": "Conflict Campaign",
            "timezone": "UTC",
            "daily_window_start_local": "09:00",
            "daily_window_end_local": "17:00",
            "max_concurrent": 1,
            "min_interval_seconds_between_calls": 0,
            "default_context": "demo",
            "voicemail_drop_mode": "upload",
            "voicemail_drop_media_uri": "sound:ai-generated/test-vm",
        }
    )
    campaign_id = campaign["id"]

    # The custom_vars JSON says score=100; the plain column says score=999.
    # JSON must win.
    csv_bytes = (
        'phone_number,score,custom_vars\n'
        '+15557002001,999,"{""score"":""100""}"\n'
    ).encode("utf-8")

    imported = await store.import_leads_csv(campaign_id, csv_bytes, skip_existing=True, max_error_rows=20)
    assert imported["accepted"] == 1

    leased = await store.lease_pending_leads(campaign_id, limit=1, lease_seconds=60)
    assert len(leased) == 1
    lead = leased[0]

    # JSON value must win
    assert lead["custom_vars"]["score"] == "100"


@pytest.mark.asyncio
async def test_standard_lead_fields_seeded_without_extra_columns(tmp_path, monkeypatch):
    """Standard lead fields are seeded into custom_vars even when no extra columns exist."""
    monkeypatch.setenv("CALL_HISTORY_ENABLED", "true")
    db_path = str(tmp_path / "seed.db")

    from src.core.outbound_store import OutboundStore

    store = OutboundStore(db_path=db_path)

    campaign = await store.create_campaign(
        {
            "name": "Seed Campaign",
            "timezone": "America/Phoenix",
            "daily_window_start_local": "09:00",
            "daily_window_end_local": "17:00",
            "max_concurrent": 1,
            "min_interval_seconds_between_calls": 0,
            "default_context": "demo",
            "voicemail_drop_mode": "upload",
            "voicemail_drop_media_uri": "sound:ai-generated/test-vm",
        }
    )
    campaign_id = campaign["id"]

    csv_bytes = (
        "name,phone_number,caller_id\n"
        "Carol,+15557003001,6800\n"
    ).encode("utf-8")

    imported = await store.import_leads_csv(campaign_id, csv_bytes, skip_existing=True, max_error_rows=20)
    assert imported["accepted"] == 1

    leased = await store.lease_pending_leads(campaign_id, limit=1, lease_seconds=60)
    lead = leased[0]
    cv = lead["custom_vars"]

    assert cv["lead_name"] == "Carol"
    assert cv["phone_number"] == "+15557003001"
    assert cv["lead_caller_id"] == "6800"
    assert cv["lead_timezone"] == "America/Phoenix"

