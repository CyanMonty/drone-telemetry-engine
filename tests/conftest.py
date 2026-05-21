"""
Root conftest: makes simulator and consumer modules importable without package prefix,
matching how each service runs inside Docker (from its own directory).
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "simulator"))
sys.path.insert(0, str(_ROOT / "consumer"))
