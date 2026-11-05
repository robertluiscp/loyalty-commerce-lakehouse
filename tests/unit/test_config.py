from loyalty_lakehouse.config import load_settings


def test_local_defaults():
    s = load_settings("local")
    assert not s.is_databricks
    assert s.schema("silver") == "silver"
    assert s.rules["credit_sla_hours"] == 72
    assert s.reference_timestamp.utcoffset().total_seconds() == -3 * 3600


def test_databricks_inherits_local_and_overrides_storage():
    s = load_settings("databricks")
    assert s.is_databricks
    assert s.storage["catalog"] == "loyalty_lakehouse"
    assert s.landing_path.startswith("/Volumes/")
    assert s.rules["attribution_window_days"] == 7  # herdado de local.yaml


def test_overrides_are_deep_merged():
    s = load_settings("local", {"rules": {"credit_sla_hours": 24}})
    assert s.rules["credit_sla_hours"] == 24
    assert s.rules["timezone"] == "America/Sao_Paulo"
