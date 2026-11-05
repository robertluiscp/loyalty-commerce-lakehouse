"""Benchmark controlado CSV x Parquet (x Delta) com a MESMA base e os MESMOS parâmetros.

Dois modos, porque medem coisas diferentes:

* ``local``  - pandas + pyarrow nesta máquina (sem Spark). Mede formato de arquivo puro.
* ``spark``  - Spark no ambiente configurado (Databricks serverless via Connect, ou local).
  Inclui overhead de rede/planejamento; os números NÃO são comparáveis com o modo local.

Metodologia (docs/benchmark_methodology.md): N repetições, mediana; leitura "completa" força
varredura de todas as colunas; leitura "parcial" lê 2 colunas e agrega. Resultados variam com
volume, compressão, hardware, cache e configuração - não generalize.
"""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import tempfile
import time
from pathlib import Path

COLUMNS_SUBSET = ["partner_id", "net_amount"]


def _dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file()) if Path(path).is_dir() \
        else Path(path).stat().st_size


def _median_time(fn, repeats: int) -> float:
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return round(statistics.median(times), 4)


def local_benchmark(source_csvs: list[Path], replicate: int = 10, repeats: int = 5) -> dict:
    """CSV x Parquet com pandas/pyarrow: tamanho, escrita, leitura completa e de 2 colunas."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    base = pd.concat([pd.read_csv(p, dtype=str, keep_default_na=False) for p in source_csvs], ignore_index=True)
    df = pd.concat([base] * replicate, ignore_index=True)
    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce")
    df["gross_amount"] = pd.to_numeric(df["gross_amount"], errors="coerce")
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce")
    work = Path(tempfile.mkdtemp(prefix="llh_bench_"))
    csv_path, pq_path = work / "purchases.csv", work / "purchases.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    try:
        results = {
            "csv": {
                "write_s": _median_time(lambda: df.to_csv(csv_path, index=False), repeats),
                "read_all_s": _median_time(lambda: pd.read_csv(csv_path), repeats),
                "read_2cols_agg_s": _median_time(
                    lambda: pd.read_csv(csv_path, usecols=COLUMNS_SUBSET).groupby("partner_id")["net_amount"].sum(),
                    repeats),
            },
            "parquet_snappy": {
                "write_s": _median_time(lambda: pq.write_table(table, pq_path, compression="snappy"), repeats),
                "read_all_s": _median_time(lambda: pq.read_table(pq_path).to_pandas(), repeats),
                "read_2cols_agg_s": _median_time(
                    lambda: pq.read_table(pq_path, columns=COLUMNS_SUBSET).to_pandas()
                    .groupby("partner_id")["net_amount"].sum(), repeats),
            },
        }
        results["csv"]["size_bytes"] = _dir_size(csv_path)
        results["parquet_snappy"]["size_bytes"] = _dir_size(pq_path)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return {
        "mode": "local (pandas + pyarrow)", "rows": len(df), "columns": len(df.columns),
        "replicate_factor": replicate, "repeats": repeats, "statistic": "mediana",
        "environment": {"python": platform.python_version(), "pandas": pd.__version__, "pyarrow": pa.__version__,
                        "machine": platform.machine(), "processor": platform.processor(), "os": platform.platform()},
        "results": results,
    }


def spark_benchmark(lh, source_table: str = "purchases", replicate: int = 10, repeats: int = 3,
                    base_path: str | None = None) -> dict:
    """CSV x Parquet x Delta com Spark no ambiente do Lakehouse (Databricks ou local)."""
    from pyspark.sql import functions as F

    spark = lh.spark
    src = lh.read(lh.table("silver", source_table))
    cols = [c for c in src.columns if not c.startswith("_")]
    # Replica a base de forma determinística (crossJoin com range) para ter volume mensurável.
    df = src.select(*cols).crossJoin(spark.range(replicate).withColumnRenamed("id", "replica"))
    rows = df.count()
    root = base_path or (f"/Volumes/{lh.catalog}/bronze/landing/_benchmark" if lh.settings.is_databricks
                         else lh.settings.resolve("data/benchmark").as_posix())
    paths = {"csv": f"{root}/csv", "parquet": f"{root}/parquet", "delta": f"{root}/delta"}

    def write(fmt: str):
        w = df.write.mode("overwrite")
        if fmt == "csv":
            w.option("header", True).csv(paths["csv"])
        else:
            w.format(fmt).save(paths[fmt])

    def reader(fmt: str):
        if fmt == "csv":
            # Schema explícito: inferir schema exigiria uma leitura extra (injusto com o CSV).
            return spark.read.schema(df.schema).option("header", True).csv(paths["csv"])
        return spark.read.format(fmt).load(paths[fmt])

    results = {}
    for fmt in ("csv", "parquet", "delta"):
        results[fmt] = {
            "write_s": _median_time(lambda: write(fmt), repeats),
            # 'noop' força a leitura/deserialização completa sem custo de escrita.
            "read_all_s": _median_time(lambda: reader(fmt).write.format("noop").mode("overwrite").save(), repeats),
            "read_2cols_agg_s": _median_time(
                lambda: reader(fmt).groupBy("partner_id").agg(F.sum("net_amount")).collect(), repeats),
        }
    sizes = _remote_sizes(lh, paths) if lh.settings.is_databricks else {k: _dir_size(Path(v)) for k, v in paths.items()}
    for fmt, size in sizes.items():
        results[fmt]["size_bytes"] = size
    # Delta guarda os arquivos das versões anteriores (time travel) até um VACUUM: após N
    # overwrites o diretório tem ~N cópias. DESCRIBE DETAIL mede só a versão atual.
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{paths['delta']}`").first()
    results["delta"]["size_current_version_bytes"] = int(detail["sizeInBytes"])
    results["delta"]["num_files_current_version"] = int(detail["numFiles"])
    results["delta"]["note"] = ("size_bytes inclui arquivos de versões anteriores (overwrites das "
                                "repetições), retidos para time travel até um VACUUM")
    return {"mode": f"spark ({lh.settings.environment})", "rows": rows, "columns": len(df.columns),
            "replicate_factor": replicate, "repeats": repeats, "statistic": "mediana", "spark_version": spark.version,
            "paths": paths, "results": results,
            "note": "No Databricks Connect os tempos incluem ida e volta de rede e planejamento remoto."}


def _remote_sizes(lh, paths: dict) -> dict:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=lh.settings.data.get("databricks", {}).get("profile", "DEFAULT"))

    def size(path: str) -> int:
        total = 0
        for entry in w.files.list_directory_contents(path):
            total += size(entry.path) if entry.is_directory else (entry.file_size or 0)
        return total
    return {k: size(v) for k, v in paths.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark CSV x Parquet (x Delta).")
    parser.add_argument("--mode", choices=["local", "spark"], default="local")
    parser.add_argument("--env", choices=["local", "databricks"], default="local")
    parser.add_argument("--replicate", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", default="docs/results/benchmark_local.json")
    args = parser.parse_args()
    from .config import load_settings
    settings = load_settings(args.env)
    if args.mode == "local":
        landing = settings.generated_path / "landing" / "purchases"
        res = local_benchmark(sorted(landing.glob("batch_id=*/purchases.csv")), args.replicate, args.repeats)
    else:
        from .utils import Lakehouse, get_spark
        res = spark_benchmark(Lakehouse(settings, get_spark(settings)), replicate=args.replicate, repeats=args.repeats)
    out = settings.resolve(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
