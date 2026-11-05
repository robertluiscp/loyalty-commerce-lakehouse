"""Executa a pipeline completa no Databricks (serverless) a partir desta máquina.

Etapas: gera os dados localmente -> envia ao Volume do Unity Catalog -> Bronze -> Silver ->
reconciliação -> qualidade -> Gold. Exige `databricks auth login` (perfil em ~/.databrickscfg)
e o ambiente virtual com requirements-databricks.txt.

Exemplos:
    python scripts/run_databricks_pipeline.py                    # volume padrão, schemas oficiais
    python scripts/run_databricks_pipeline.py --profile small --dev   # dataset pequeno em schemas dev_*
    python scripts/run_databricks_pipeline.py --skip-generate --batch B002_20250701   # reprocessa um lote
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from loyalty_lakehouse.config import load_settings  # noqa: E402
from loyalty_lakehouse.data_generator import SMALL_PROFILE, generate_to_disk  # noqa: E402
from loyalty_lakehouse.pipeline import reset_tables, run_pipeline  # noqa: E402

DEV_OVERRIDES = {
    "storage": {
        "landing_path": "/Volumes/loyalty_lakehouse/bronze/landing/_dev",
        "schemas": {"bronze": "dev_bronze", "silver": "dev_silver", "gold": "dev_gold",
                    "governance": "dev_governance"},
    },
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=["default", "small"], default="default")
    parser.add_argument("--dev", action="store_true", help="usa schemas dev_* e uma subpasta do Volume")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-gold", action="store_true")
    parser.add_argument("--batch", action="append")
    parser.add_argument("--reset", action="store_true",
                        help="remove as tabelas dos schemas configurados antes de rodar (dados regenerados)")
    args = parser.parse_args()

    overrides: dict = {}
    if args.dev:
        overrides.update(DEV_OVERRIDES)
    if args.profile == "small":
        overrides["generator"] = {**SMALL_PROFILE, "output_path": "data/generated_small"}
    settings = load_settings("databricks", overrides)
    if not args.skip_generate:
        report = generate_to_disk(settings.generator, settings.rules, settings.generated_path)
        print("Dados gerados:", json.dumps(report["anomalies_injected"]))
    if args.reset:
        from loyalty_lakehouse.utils import get_spark
        reset_tables(settings, get_spark(settings))
    start = time.time()
    res = run_pipeline(settings, batches=args.batch, upload=not args.skip_upload,
                       build_gold_layer=not args.skip_gold)
    print(json.dumps({"run_id": res.run_id, "seconds": round(time.time() - start), "batches": res.batches,
                      "silver": res.silver, "reconciliation": res.reconciliation, "gold": res.gold},
                     indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
