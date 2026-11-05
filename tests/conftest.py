"""Fixtures compartilhadas.

Grupos de teste (ver seção de testes no README):
* ``tests/unit`` sem marcador: Python puro (gerador, configuração).
* ``tests/unit`` com ``@pytest.mark.spark``: SparkSession LOCAL em memória (sem gravar arquivos,
  funciona até no Windows sem winutils). Exige Java 17 e o ambiente com ``pyspark``.
* ``tests/integration``: ``delta_local`` (Delta Lake local) e ``databricks`` (serverless).
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))


def _has_databricks_connect() -> bool:
    try:
        import databricks.connect  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.fixture(scope="session")
def spark():
    """SparkSession local, sem Delta, para testes de lógica com DataFrames em memória."""
    pytest.importorskip("pyspark")
    if _has_databricks_connect():
        pytest.skip("ambiente com databricks-connect: rode os testes 'spark' no ambiente com pyspark local")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Java não encontrado (defina JAVA_HOME)")
    from pyspark.sql import SparkSession
    # Dados minúsculos: 1 partição evita overhead de agendamento (o padrão de 200 seria absurdo aqui).
    session = (SparkSession.builder.master("local[1]").appName("llh-tests")
               .config("spark.sql.shuffle.partitions", "1").config("spark.default.parallelism", "1")
               .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true")
               .config("spark.driver.memory", "2g").config("spark.sql.maxPlanStringLength", "100000")
               .getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="session")
def small_dataset(tmp_path_factory):
    """Dataset pequeno e determinístico gerado uma vez por sessão de testes."""
    from loyalty_lakehouse.config import load_settings
    from loyalty_lakehouse.data_generator import SMALL_PROFILE, generate_to_disk
    settings = load_settings("local")
    cfg = {**settings.generator, **SMALL_PROFILE}
    out = tmp_path_factory.mktemp("small")
    report = generate_to_disk(cfg, settings.rules, out)
    return {"path": out, "report": report, "cfg": cfg, "rules": settings.rules}
