"""Acesso aos arquivos de origem (landing zone) e leitura bruta com Spark.

Landing zone:
``<landing>/<entidade>/batch_id=<lote>/<entidade>.<csv|jsonl>`` + ``<landing>/_manifests/<lote>.json``.

O manifesto funciona como "arquivo de controle" do sistema de origem: lista os arquivos
esperados no lote e quantas linhas cada um tem. Ele permite detectar arquivo ausente e
divergência de contagem entre origem e Bronze.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, functions as F, types as T

from .config import Settings
from .schemas import SourceEntity
from .utils import log, running_inside_databricks

CORRUPT_COL = "_corrupt_record"


class SourceFiles:
    """Lista/lê arquivos do landing local, do Volume (dentro do Databricks) ou via SDK (remoto)."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.root = settings.landing_path.rstrip("/")
        self.remote = settings.is_databricks and not running_inside_databricks()
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(profile=self.settings.data.get("databricks", {}).get("profile", "DEFAULT"))
        return self._client

    def path(self, relative: str) -> str:
        return f"{self.root}/{relative}"

    def exists(self, relative: str) -> bool:
        full = self.path(relative)
        if not self.remote:
            return os.path.exists(full)
        try:
            self.client.files.get_metadata(full)
            return True
        except Exception:
            return False

    def read_text(self, relative: str) -> str:
        full = self.path(relative)
        if not self.remote:
            return Path(full).read_text(encoding="utf-8")
        return self.client.files.download(full).contents.read().decode("utf-8")

    def list_batches(self) -> list[str]:
        folder = self.path("_manifests")
        if not self.remote:
            names = os.listdir(folder) if os.path.isdir(folder) else []
        else:
            try:
                names = [Path(e.path).name for e in self.client.files.list_directory_contents(folder)]
            except Exception:
                names = []
        return sorted(n[:-5] for n in names if n.endswith(".json"))

    def manifest(self, batch_id: str) -> dict:
        return json.loads(self.read_text(f"_manifests/{batch_id}.json"))

    def upload_from_local(self, local_landing: Path) -> int:
        """Envia o landing local para o Volume do Unity Catalog (somente modo remoto)."""
        if not self.remote:
            raise RuntimeError("upload_from_local só se aplica ao modo databricks remoto")
        count = 0
        for file in sorted(Path(local_landing).rglob("*")):
            if file.is_file():
                rel = file.relative_to(local_landing).as_posix()
                with file.open("rb") as fh:
                    self.client.files.upload(self.path(rel), fh, overwrite=True)
                count += 1
        log.info("%d arquivos enviados para %s", count, self.root)
        return count


def string_schema(columns) -> T.StructType:
    return T.StructType([T.StructField(c, T.StringType(), True) for c in columns])


def read_raw(spark, entity: SourceEntity, path: str) -> tuple[DataFrame | None, list[dict]]:
    """Lê um arquivo de origem preservando os valores como texto.

    Retorna ``(df, issues)``. ``df`` é ``None`` quando o arquivo não pode ser ingerido
    (por exemplo, faltam colunas obrigatórias). ``issues`` descreve problemas de schema.
    """
    issues: list[dict] = []
    if entity.file_format == "csv":
        # Lê só o cabeçalho (sem inferir tipos) para comparar com o contrato.
        header = spark.read.option("header", True).csv(path).columns
        missing = [c for c in entity.columns if c not in header]
        extra = [c for c in header if c not in entity.columns]
        if missing:
            issues.append({"category": "MISSING_COLUMNS", "severity": "ERROR",
                           "description": f"{entity.name}: colunas ausentes {missing}; arquivo não ingerido"})
            return None, issues
        if extra:
            issues.append({"category": "UNEXPECTED_COLUMNS", "severity": "WARNING",
                           "description": f"{entity.name}: colunas extras {extra} preservadas em _extra_columns"})
        # Schema explícito na ORDEM DO CABEÇALHO: evita desalinhar colunas por posição.
        schema = string_schema(list(header) + [CORRUPT_COL])
        raw = (spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", CORRUPT_COL).csv(path)
               .select("*", F.col("_metadata.file_path").alias("_source_file")))
        extra_col = F.to_json(F.struct(*[F.col(c) for c in extra])) if extra else F.lit(None).cast("string")
    else:
        keys = [r[0] for r in spark.read.text(path)
                .select(F.explode(F.json_object_keys("value")).alias("k")).distinct().collect()]
        extra = sorted(k for k in keys if k not in entity.columns)
        if extra:
            issues.append({"category": "UNEXPECTED_COLUMNS", "severity": "WARNING",
                           "description": f"{entity.name}: chaves JSON extras {extra} (mantidas só no arquivo de origem)"})
        schema = string_schema(list(entity.columns) + [CORRUPT_COL])
        raw = (spark.read.schema(schema).option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", CORRUPT_COL).json(path)
               .select("*", F.col("_metadata.file_path").alias("_source_file")))
        extra_col = F.lit(None).cast("string")
    hash_input = F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("∅")) for c in entity.columns])
    df = raw.select(
        *[F.col(c) for c in entity.columns],
        extra_col.alias("_extra_columns"),
        F.col(CORRUPT_COL),
        F.col("_source_file"),
        F.sha2(hash_input, 256).alias("_record_hash"),
    )
    return df, issues
