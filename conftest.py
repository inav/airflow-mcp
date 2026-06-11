"""Pytest conftest — make ``src/`` importable without installing the package."""
import os
import sys
from pathlib import Path

_SRC = Path(__file__).parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Ensure build_server() (which validates settings) can construct Settings().
os.environ.setdefault("AIRFLOW_BASE_URL", "http://localhost:8080")
os.environ.setdefault("AIRFLOW_USERNAME", "test")
os.environ.setdefault("AIRFLOW_PASSWORD", "test")

