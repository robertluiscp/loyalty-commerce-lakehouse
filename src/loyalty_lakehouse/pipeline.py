"""Orquestração: landing -> Bronze -> Silver -> reconciliação -> qualidade -> Gold.

Uso (CLI):
    python -m loyalty_lakehouse.pipeline --env databricks            # todos os lotes
    python -m loyalty_lakehouse.pipeline --env databricks --batch B002_20250701   # reprocessa 1 lote
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field

from .bronze import ingest_batch
from .config import LAYERS, Settings, load_settings
from .gold import build_gold
from .governance import GovernanceRecorder
from .ingestion import SourceFiles
from .quality import evaluate_quality
from .reconciliation import run_reconciliation
from .schemas import ENTITIES
from .silver import Context, process_entity
from .utils import Lakehouse, get_spark, log, new_run_id, utc_now


@dataclass
class PipelineResult:
    run_id: str
    batches: list[str]
    bronze: dict = field(default_factory=dict)
    silver: dict = field(default_factory=dict)
    reconciliation: dict = field(default_factory=dict)
    gold: dict = field(default_factory=dict)


def reset_tables(settings: Settings, spark) -> list[str]:
    """Remove TODAS as tabelas dos 4 schemas configurados (não remove schemas nem o Volume).

    Uso explícito (``--reset``) para recomeçar do zero após regenerar os dados.
    """
    lh = Lakehouse(settings, spark)
    dropped = []
    for layer in LAYERS:
        if settings.is_databricks:
            schema = f"{lh.catalog}.{settings.schema(layer)}"
            if not spark.catalog.databaseExists(schema):
                continue
            for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
                if not row["isTemporary"]:
                    spark.sql(f"DROP TABLE IF EXISTS {schema}.{row['tableName']}")
                    dropped.append(f"{schema}.{row['tableName']}")
        else:
            import shutil
            folder = lh.warehouse / settings.schema(layer)
            if folder.exists():
                shutil.rmtree(folder)
                dropped.append(str(folder))
    log.info("%d tabelas removidas", len(dropped))
    return dropped


def run_pipeline(settings: Settings, spark=None, batches: list[str] | None = None, upload: bool = False,
                 build_gold_layer: bool = True) -> PipelineResult:
    spark = spark or get_spark(settings)
    lh = Lakehouse(settings, spark)
    lh.ensure_schemas()
    run_id, run_ts = new_run_id(), utc_now()
    gov = GovernanceRecorder(lh, run_id)
    gov.ensure_tables()
    source = SourceFiles(settings)
    result = PipelineResult(run_id, [])
    try:
        if upload:
            with gov.step("upload_landing") as info:
                info["rows_written"] = source.upload_from_local(settings.resolve(settings.generator["output_path"]) / "landing")
        available = source.list_batches()
        result.batches = sorted(batches) if batches else available
        missing = [b for b in result.batches if b not in available]
        if missing:
            raise FileNotFoundError(f"lotes sem manifesto no landing: {missing}")
        ctx = Context(lh, settings.rules["timezone"], float(settings.rules["credit_sla_hours"]))
        for batch in result.batches:
            result.bronze[batch] = ingest_batch(lh, gov, source, batch, run_ts)
            result.silver[batch] = {e.name: process_entity(lh, gov, ctx, e.name, batch, run_ts) for e in ENTITIES}
            gov.flush()
        result.reconciliation = run_reconciliation(lh, gov)
        evaluate_quality(lh, gov, result.batches, result.reconciliation)
        gov.flush()
        if build_gold_layer:
            result.gold = build_gold(lh, gov)
    finally:
        gov.flush()
    log.info("Execução %s concluída", run_id)
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Executa a pipeline do Loyalty Commerce Lakehouse.")
    parser.add_argument("--env", default="local", choices=["local", "databricks"])
    parser.add_argument("--batch", action="append", help="lote(s) a processar; padrão: todos")
    parser.add_argument("--upload", action="store_true", help="envia data/generated/landing ao Volume (databricks)")
    parser.add_argument("--skip-gold", action="store_true")
    args = parser.parse_args(argv)
    settings = load_settings(args.env)
    res = run_pipeline(settings, batches=args.batch, upload=args.upload, build_gold_layer=not args.skip_gold)
    print(json.dumps({"run_id": res.run_id, "batches": res.batches, "bronze": res.bronze,
                      "silver": res.silver, "reconciliation": res.reconciliation, "gold": res.gold},
                     indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
