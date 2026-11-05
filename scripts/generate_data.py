"""Gera os dados sintéticos em data/generated (wrapper do módulo data_generator).

    python scripts/generate_data.py                 # volume padrão (config/local.yaml)
    python scripts/generate_data.py --profile small # conjunto pequeno para testes
    python scripts/generate_data.py --purchases 300000 --customers 30000 --seed 123
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from loyalty_lakehouse.data_generator import main  # noqa: E402

if __name__ == "__main__":
    main()
