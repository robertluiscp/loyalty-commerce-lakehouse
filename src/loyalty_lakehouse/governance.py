"""Registros operacionais do pipeline (schema ``governance``).

Importante: estas tabelas são uma escolha de organização DO PROJETO. Elas não substituem
os mecanismos nativos de governança da plataforma (privilégios do Unity Catalog, lineage,
system tables de auditoria). Ver docs/governance.md.
"""
from __future__ import annotations

import traceback
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from pyspark.sql import types as T

from .utils import Lakehouse, log, utc_now

PIPELINE_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("step", T.StringType(), False),
    T.StructField("entity", T.StringType(), True),
    T.StructField("status", T.StringType(), False),   # SUCCESS | FAILED | SKIPPED
    T.StructField("started_at", T.TimestampType(), False),
    T.StructField("finished_at", T.TimestampType(), True),
    T.StructField("duration_seconds", T.DoubleType(), True),
    T.StructField("rows_read", T.LongType(), True),
    T.StructField("rows_written", T.LongType(), True),
    T.StructField("rows_quarantined", T.LongType(), True),
    T.StructField("message", T.StringType(), True),
    T.StructField("environment", T.StringType(), False),
])

DQ_RESULTS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("entity", T.StringType(), False),
    T.StructField("check_name", T.StringType(), False),
    T.StructField("check_category", T.StringType(), False),
    T.StructField("population", T.StringType(), False),
    T.StructField("numerator", T.LongType(), True),
    T.StructField("denominator", T.LongType(), True),
    T.StructField("metric_value", T.DoubleType(), True),
    T.StructField("threshold", T.DoubleType(), True),
    T.StructField("comparator", T.StringType(), True),  # "<=" ou ">=" (ou null = informativo)
    T.StructField("passed", T.BooleanType(), True),
    T.StructField("severity", T.StringType(), False),
    T.StructField("checked_at", T.TimestampType(), False),
])

INCIDENTS_SCHEMA = T.StructType([
    T.StructField("incident_id", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("entity", T.StringType(), True),
    T.StructField("category", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),   # INFO | WARNING | ERROR
    T.StructField("description", T.StringType(), False),
    T.StructField("affected_records", T.LongType(), True),
    T.StructField("source_reference", T.StringType(), True),
    T.StructField("detected_at", T.TimestampType(), False),
    T.StructField("status", T.StringType(), False),     # OPEN
])

RECONCILIATION_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("check_name", T.StringType(), False),
    T.StructField("entity", T.StringType(), False),
    T.StructField("business_key", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("expected_value", T.DoubleType(), True),
    T.StructField("actual_value", T.DoubleType(), True),
    T.StructField("difference", T.DoubleType(), True),
    T.StructField("status", T.StringType(), False),     # DIVERGENT | INFO | OK
    T.StructField("details", T.StringType(), True),
    T.StructField("audited_at", T.TimestampType(), False),
])

GOVERNANCE_TABLES = {
    "pipeline_execution_log": PIPELINE_LOG_SCHEMA,
    "data_quality_results": DQ_RESULTS_SCHEMA,
    "data_quality_incidents": INCIDENTS_SCHEMA,
    "reconciliation_audit": RECONCILIATION_SCHEMA,
}


class GovernanceRecorder:
    """Acumula registros em memória e grava em lote (append) nas tabelas de governança."""

    def __init__(self, lh: Lakehouse, run_id: str):
        self.lh = lh
        self.run_id = run_id
        self.env = lh.settings.environment
        self._buffer: dict[str, list[dict]] = {name: [] for name in GOVERNANCE_TABLES}
        self._incident_seq = 0

    def ensure_tables(self) -> None:
        spark = self.lh.spark
        for name, schema in GOVERNANCE_TABLES.items():
            self.lh.create_if_missing(spark.createDataFrame([], schema), self.lh.table("governance", name))

    # ---- registros -----------------------------------------------------------
    def incident(self, category: str, severity: str, description: str, *, batch_id: str | None = None,
                 entity: str | None = None, affected: int | None = None, source: str | None = None) -> None:
        self._incident_seq += 1
        self._buffer["data_quality_incidents"].append({
            "incident_id": f"{self.run_id}-{self._incident_seq:04d}", "run_id": self.run_id,
            "batch_id": batch_id, "entity": entity, "category": category, "severity": severity,
            "description": description, "affected_records": affected, "source_reference": source,
            "detected_at": utc_now(), "status": "OPEN",
        })
        log.warning("[incidente %s/%s] %s", category, severity, description)

    def dq_result(self, row: dict[str, Any]) -> None:
        self._buffer["data_quality_results"].append({"run_id": self.run_id, "checked_at": utc_now(), **row})

    def reconciliation_record(self, row: dict[str, Any]) -> None:
        self._buffer["reconciliation_audit"].append({"run_id": self.run_id, "audited_at": utc_now(), **row})

    def reconciliation_rows(self, df) -> None:
        """Grava diretamente um DataFrame de divergências (sem trazer as linhas para o driver)."""
        from pyspark.sql import functions as F
        out = df.withColumn("run_id", F.lit(self.run_id)).withColumn("audited_at", F.lit(utc_now()))
        out = out.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in RECONCILIATION_SCHEMA.fields])
        self.lh.write(out, self.lh.table("governance", "reconciliation_audit"), mode="append")

    @contextmanager
    def step(self, step: str, *, batch_id: str | None = None, entity: str | None = None) -> Iterator[dict]:
        """Registra início, fim, duração, contagens e falhas de uma etapa."""
        info: dict[str, Any] = {"rows_read": None, "rows_written": None, "rows_quarantined": None,
                                "message": None, "status": "SUCCESS"}
        started: datetime = utc_now()
        log.info("-> %s %s %s", step, batch_id or "", entity or "")
        try:
            yield info
        except Exception as exc:  # registra e propaga
            info["status"] = "FAILED"
            info["message"] = f"{type(exc).__name__}: {exc}"[:2000]
            log.error("Falha em %s: %s\n%s", step, exc, traceback.format_exc(limit=3))
            raise
        finally:
            finished = utc_now()
            self._buffer["pipeline_execution_log"].append({
                "run_id": self.run_id, "batch_id": batch_id, "step": step, "entity": entity,
                "status": info["status"], "started_at": started, "finished_at": finished,
                "duration_seconds": (finished - started).total_seconds(),
                "rows_read": info["rows_read"], "rows_written": info["rows_written"],
                "rows_quarantined": info["rows_quarantined"], "message": info["message"],
                "environment": self.env,
            })

    def flush(self) -> None:
        spark = self.lh.spark
        for name, rows in self._buffer.items():
            if rows:
                schema = GOVERNANCE_TABLES[name]
                tuples = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
                df = spark.createDataFrame(tuples, schema)
                self.lh.write(df, self.lh.table("governance", name), mode="append")
                self._buffer[name] = []
