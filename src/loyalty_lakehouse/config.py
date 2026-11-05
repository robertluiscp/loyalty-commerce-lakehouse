"""Carregamento de configuração (YAML) para os ambientes local e Databricks.

``config/local.yaml`` é a base. O arquivo do ambiente (``config/databricks.yaml`` ou,
na falta dele, ``config/databricks.example.yaml``) sobrescreve apenas as chaves que define.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
LAYERS = ("bronze", "silver", "gold", "governance")


def _deep_merge(base: dict, override: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        elif value is not None or key not in result:
            result[key] = value
    return result


def _load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@dataclass
class Settings:
    environment: str
    data: dict[str, Any]
    root: Path = field(default=REPO_ROOT)

    # ---- atalhos -------------------------------------------------------
    @property
    def is_databricks(self) -> bool:
        return self.environment == "databricks"

    @property
    def generator(self) -> dict[str, Any]:
        return self.data["generator"]

    @property
    def rules(self) -> dict[str, Any]:
        return self.data["rules"]

    @property
    def thresholds(self) -> dict[str, float]:
        return self.data["quality_thresholds"]

    @property
    def storage(self) -> dict[str, Any]:
        return self.data["storage"]

    @property
    def reference_timestamp(self) -> datetime:
        return datetime.fromisoformat(self.rules["reference_timestamp"])

    def resolve(self, path: str | Path) -> Path:
        p = Path(path)
        return p if p.is_absolute() else (self.root / p).resolve()

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Camada desconhecida: {layer}")
        return self.storage["schemas"][layer]

    @property
    def landing_path(self) -> str:
        """Diretório raiz dos arquivos de origem (local ou Volume do Unity Catalog)."""
        lp = self.storage["landing_path"]
        return lp if self.is_databricks else self.resolve(lp).as_posix()

    @property
    def generated_path(self) -> Path:
        return self.resolve(self.generator["output_path"])


def load_settings(environment: str = "local", overrides: dict | None = None) -> Settings:
    """Lê a configuração do ambiente e aplica ``overrides`` (útil em testes)."""
    data = _load_yaml(CONFIG_DIR / "local.yaml")
    if environment == "databricks":
        custom = CONFIG_DIR / "databricks.yaml"
        env_file = custom if custom.exists() else CONFIG_DIR / "databricks.example.yaml"
        data = _deep_merge(data, _load_yaml(env_file))
    elif environment != "local":
        raise ValueError("environment deve ser 'local' ou 'databricks'")
    if overrides:
        data = _deep_merge(data, overrides)
    data["environment"] = environment
    return Settings(environment=environment, data=data)
