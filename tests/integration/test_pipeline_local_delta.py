"""Integração local com Delta Lake (pyspark + delta-spark).

Roda em Linux/macOS/WSL e no GitHub Actions. No Windows, o Spark precisa de ``winutils.exe``
e ``hadoop.dll`` (HADOOP_HOME) para gravar arquivos; sem isso o teste é pulado.
"""
from __future__ import annotations

import os
import sys

import pytest

from pipeline_assertions import run_and_check

pytestmark = pytest.mark.delta_local


@pytest.fixture(scope="module")
def local_env(tmp_path_factory):
    pytest.importorskip("delta")
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("Windows sem HADOOP_HOME/winutils: Spark local não grava arquivos (ver docs/limitations.md)")
    try:
        import databricks.connect  # noqa: F401
        pytest.skip("ambiente com databricks-connect; use o ambiente com pyspark local")
    except ImportError:
        pass
    from loyalty_lakehouse.config import load_settings
    from loyalty_lakehouse.data_generator import SMALL_PROFILE, generate_to_disk
    from loyalty_lakehouse.utils import get_spark
    root = tmp_path_factory.mktemp("local_it")
    settings = load_settings("local", {
        "generator": {**SMALL_PROFILE, "output_path": str(root / "generated")},
        "storage": {"landing_path": str(root / "generated" / "landing"), "warehouse_path": str(root / "lakehouse")},
        "spark": {"master": "local[2]", "shuffle_partitions": 2}})
    generate_to_disk(settings.generator, settings.rules, settings.generated_path)
    spark = get_spark(settings)
    yield settings, spark, settings.generated_path / "anomaly_ledger.csv"
    spark.stop()


def test_end_to_end_local_delta(local_env):
    settings, spark, ledger = local_env
    run_and_check(settings, spark, ledger)
