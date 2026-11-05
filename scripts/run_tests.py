"""Atalho para os grupos de teste.

    python scripts/run_tests.py unit         # Python puro (sem Spark)
    python scripts/run_tests.py spark        # regras com Spark local em memória (Java 17)
    python scripts/run_tests.py delta        # integração local com Delta (Linux/macOS/WSL/CI)
    python scripts/run_tests.py databricks   # integração no Databricks (cria schemas it_*)
    python scripts/run_tests.py all-local    # unit + spark + delta
"""
import os
import subprocess
import sys

GROUPS = {
    "unit": ["tests/unit", "-m", "not spark"],
    "spark": ["tests/unit", "-m", "spark"],
    "delta": ["tests/integration", "-m", "delta_local"],
    "databricks": ["tests/integration", "-m", "databricks"],
    "all-local": ["tests", "-m", "not databricks"],
}

if __name__ == "__main__":
    group = sys.argv[1] if len(sys.argv) > 1 else "unit"
    if group not in GROUPS:
        sys.exit(f"grupo inválido: {group}. Opções: {', '.join(GROUPS)}")
    env = dict(os.environ)
    if group == "databricks":
        env["LLH_RUN_DATABRICKS_TESTS"] = "1"
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", *GROUPS[group], *sys.argv[2:]], env=env))
