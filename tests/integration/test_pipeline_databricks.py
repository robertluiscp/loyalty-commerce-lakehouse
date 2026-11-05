"""Integração no Databricks (serverless) via Databricks Connect.

Opt-in: só roda com ``LLH_RUN_DATABRICKS_TESTS=1`` (cria objetos no workspace).
Usa schemas próprios ``it_*`` no catálogo e uma subpasta do Volume; ao final remove os
schemas ``it_*`` (somente eles), a menos que ``LLH_KEEP_TEST_SCHEMAS=1``.
"""
from __future__ import annotations

import os

import pytest

from pipeline_assertions import run_and_check

pytestmark = pytest.mark.databricks


@pytest.fixture(scope="module")
def dbx(tmp_path_factory):
    if os.environ.get("LLH_RUN_DATABRICKS_TESTS") != "1":
        pytest.skip("defina LLH_RUN_DATABRICKS_TESTS=1 para rodar no Databricks")
    pytest.importorskip("databricks.connect")
    from loyalty_lakehouse.config import load_settings
    from loyalty_lakehouse.data_generator import SMALL_PROFILE, generate_to_disk
    from loyalty_lakehouse.utils import get_spark
    out = tmp_path_factory.mktemp("dbx_it")
    schemas = {"bronze": "it_bronze", "silver": "it_silver", "gold": "it_gold", "governance": "it_governance"}
    settings = load_settings("databricks", {
        "generator": {**SMALL_PROFILE, "output_path": str(out)},
        "storage": {"landing_path": "/Volumes/loyalty_lakehouse/bronze/landing/_it", "schemas": schemas}})
    generate_to_disk(settings.generator, settings.rules, out)
    spark = get_spark(settings)
    yield settings, spark, out / "anomaly_ledger.csv"
    if os.environ.get("LLH_KEEP_TEST_SCHEMAS") != "1":
        for s in schemas.values():
            spark.sql(f"DROP SCHEMA IF EXISTS {settings.storage['catalog']}.{s} CASCADE")


def test_end_to_end_on_databricks(dbx):
    settings, spark, ledger = dbx
    run_and_check(settings, spark, ledger)
