import json
from pathlib import Path


def test_quality_rules_config_is_valid():
    config = json.loads(Path("config/quality_rules.json").read_text(encoding="utf-8"))
    assert config["rules"]
    assert all({"rule_id","column","type","severity"} <= set(rule) for rule in config["rules"])


def test_survivorship_config_has_attribute_priorities():
    config = json.loads(Path("config/survivorship.json").read_text(encoding="utf-8"))
    assert "name" in config["attribute_source_precedence"]
    assert "address_normalized" in config["attribute_source_precedence"]


def test_sample_data_covers_quality_and_matching_cases():
    text = Path("data/customers.csv").read_text(encoding="utf-8").lower()
    assert "asha.rao@example.com" in text
    assert "not-an-email" in text
    assert "billing" in text
