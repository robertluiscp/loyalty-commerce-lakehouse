"""Executa a pipeline completa com Spark LOCAL + Delta Lake (tabelas Delta por caminho).

Requisitos: Java 17, ambiente com requirements.txt (pyspark + delta-spark) e um sistema que
permita ao Spark gravar arquivos localmente (Linux/macOS/WSL; no Windows é preciso winutils -
ver docs/limitations.md). Na primeira execução o delta-spark baixa os JARs do Maven Central.

    python scripts/run_local_pipeline.py              # gera os dados e processa todos os lotes
    python scripts/run_local_pipeline.py --profile small
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from loyalty_lakehouse.config import load_settings  # noqa: E402
from loyalty_lakehouse.data_generator import SMALL_PROFILE, generate_to_disk  # noqa: E402
from loyalty_lakehouse.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", choices=["default", "small"], default="default")
    p.add_argument("--skip-generate", action="store_true")
    p.add_argument("--batch", action="append")
    a = p.parse_args()
    overrides = {"generator": dict(SMALL_PROFILE)} if a.profile == "small" else None
    settings = load_settings("local", overrides)
    if not a.skip_generate:
        generate_to_disk(settings.generator, settings.rules, settings.generated_path)
    res = run_pipeline(settings, batches=a.batch)
    print(json.dumps({"run_id": res.run_id, "silver": res.silver, "reconciliation": res.reconciliation,
                      "gold": res.gold}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
