"""Infraestrutura comum: SparkSession, nomes de tabelas e leitura/escrita Delta.

O mesmo código de transformação roda em dois "motores":

* **local**: ``pyspark`` + ``delta-spark``; tabelas Delta por caminho (``delta.`<path>```),
  sem metastore persistente;
* **databricks**: Databricks Connect (serverless) ou notebook no workspace; tabelas Delta
  gerenciadas pelo Unity Catalog (``catalogo.schema.tabela``).
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .config import LAYERS, Settings

log = logging.getLogger("loyalty_lakehouse")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def new_run_id() -> str:
    return f"run_{utc_now():%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"


def running_inside_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def get_spark(settings: Settings):
    """Cria (ou reaproveita) a SparkSession adequada ao ambiente configurado."""
    if settings.is_databricks:
        if running_inside_databricks():
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        else:
            from databricks.connect import DatabricksSession
            dbx = settings.data.get("databricks", {})
            builder = DatabricksSession.builder.profile(dbx.get("profile", "DEFAULT"))
            if dbx.get("serverless", True):
                builder = builder.serverless(True)
            spark = builder.getOrCreate()
    else:
        from pyspark.sql import SparkSession
        sp = settings.data["spark"]
        builder = (
            SparkSession.builder.master(sp.get("master", "local[*]"))
            .appName(sp.get("app_name", "loyalty-commerce-lakehouse"))
            .config("spark.sql.shuffle.partitions", str(sp.get("shuffle_partitions", 8)))
            .config("spark.ui.enabled", "false")
            .config("spark.sql.ansi.enabled", "true")
        )
        if sp.get("enable_delta", True):
            from delta import configure_spark_with_delta_pip
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            builder = configure_spark_with_delta_pip(builder)
        spark = builder.getOrCreate()
    # Todos os timestamps são armazenados como instantes UTC; datas de negócio são
    # derivadas explicitamente no fuso configurado (rules.timezone).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


# ---------------------------------------------------------------------------
# Tabelas
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TableRef:
    layer: str
    name: str
    fqn: str | None = None   # Unity Catalog: catalogo.schema.tabela
    path: str | None = None  # local: caminho da tabela Delta

    @property
    def sql(self) -> str:
        """Identificador utilizável em SQL nos dois ambientes."""
        return self.fqn if self.fqn else f"delta.`{self.path}`"

    def __str__(self) -> str:
        return self.fqn or f"{self.layer}.{self.name}"


class Lakehouse:
    """Resolve nomes de tabelas e encapsula leitura/escrita Delta nos dois ambientes."""

    def __init__(self, settings: Settings, spark):
        self.settings = settings
        self.spark = spark
        self.catalog = settings.storage.get("catalog")
        # Cache de existência: cada tableExists remoto custa uma ida e volta ao serverless.
        self._known_tables: set[str] = set()
        if not settings.is_databricks:
            self.warehouse = settings.resolve(settings.storage["warehouse_path"])

    def table(self, layer: str, name: str) -> TableRef:
        schema = self.settings.schema(layer)
        if self.settings.is_databricks:
            return TableRef(layer, name, fqn=f"{self.catalog}.{schema}.{name}")
        return TableRef(layer, name, path=(self.warehouse / schema / name).as_posix())

    def ensure_schemas(self) -> None:
        """Databricks: CREATE SCHEMA IF NOT EXISTS (o catálogo é criado em sql/unity_catalog_setup.sql)."""
        if self.settings.is_databricks:
            for layer in LAYERS:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.settings.schema(layer)}")
        else:
            for layer in LAYERS:
                (self.warehouse / self.settings.schema(layer)).mkdir(parents=True, exist_ok=True)

    def exists(self, ref: TableRef) -> bool:
        if ref.sql in self._known_tables:
            return True
        found = (self.spark.catalog.tableExists(ref.fqn) if ref.fqn
                 else Path(ref.path, "_delta_log").exists())
        if found:
            self._known_tables.add(ref.sql)
        return found

    def read(self, ref: TableRef):
        if ref.fqn:
            return self.spark.read.table(ref.fqn)
        return self.spark.read.format("delta").load(ref.path)

    def write(self, df, ref: TableRef, mode: str = "overwrite", replace_where: str | None = None,
              merge_schema: bool = False, overwrite_schema: bool = False) -> None:
        writer = df.write.format("delta").mode(mode)
        if replace_where and self.exists(ref):
            writer = writer.option("replaceWhere", replace_where)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if ref.fqn:
            writer.saveAsTable(ref.fqn)
        else:
            writer.save(ref.path)
        self._known_tables.add(ref.sql)

    def create_if_missing(self, df, ref: TableRef) -> None:
        """Cria a tabela vazia com o schema de ``df`` (necessário antes de um MERGE)."""
        if not self.exists(ref):
            self.write(df.limit(0), ref, mode="overwrite")

    def drop(self, ref: TableRef) -> None:
        self._known_tables.discard(ref.sql)
        if ref.fqn:
            self.spark.sql(f"DROP TABLE IF EXISTS {ref.fqn}")
        else:
            import shutil
            shutil.rmtree(ref.path, ignore_errors=True)

    def temp_view(self, df, name: str) -> str:
        df.createOrReplaceTempView(name)
        return name


@contextmanager
def timed(label: str) -> Iterator[dict]:
    info: dict = {"label": label}
    start = time.perf_counter()
    try:
        yield info
    finally:
        info["seconds"] = round(time.perf_counter() - start, 3)
