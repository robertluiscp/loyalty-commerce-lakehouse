"""Executa os arquivos de sql/ no ambiente configurado e salva um resumo dos resultados.

Cada instrução é executada com ``spark.sql``; o resultado (até 25 linhas) vai para
``docs/results/sql_checks.json``. Serve para provar que as consultas documentadas rodam.

    python scripts/run_sql_checks.py --env databricks
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from loyalty_lakehouse.config import load_settings  # noqa: E402
from loyalty_lakehouse.utils import get_spark  # noqa: E402

FILES = ["unity_catalog_setup.sql", "business_kpis.sql", "data_quality.sql", "reconciliation.sql"]


def statements(text: str) -> list[tuple[str, str]]:
    """Separa instruções por ';' ignorando linhas de comentário; devolve (rótulo, sql)."""
    out, label, buf = [], "", []
    for line in text.splitlines():
        stripped = line.strip()
        m = re.match(r"--\s*\[(\w+)\]\s*(.*)", stripped)
        if m:
            label = f"{m.group(1)} {m.group(2)}".strip()
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            sql = "\n".join(buf).rstrip().rstrip(";")
            out.append((label or sql.split("\n")[0][:60], sql))
            buf, label = [], ""
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="databricks", choices=["local", "databricks"])
    p.add_argument("--output", default="docs/results/sql_checks.json")
    a = p.parse_args()
    settings = load_settings(a.env)
    spark = get_spark(settings)
    report = []
    for name in FILES:
        for label, sql in statements((ROOT / "sql" / name).read_text(encoding="utf-8")):
            start = time.perf_counter()
            try:
                df = spark.sql(sql)
                rows = [r.asDict() for r in df.limit(25).collect()]
                report.append({"file": name, "statement": label, "status": "OK", "rows_returned": len(rows),
                               "seconds": round(time.perf_counter() - start, 2), "sample": rows})
            except Exception as exc:
                report.append({"file": name, "statement": label, "status": "ERROR",
                               "error": f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"})
            print(f"[{report[-1]['status']}] {name} :: {label}")
    out = ROOT / a.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    errors = [r for r in report if r["status"] != "OK"]
    print(f"{len(report) - len(errors)} OK, {len(errors)} com erro -> {out}")


if __name__ == "__main__":
    main()
