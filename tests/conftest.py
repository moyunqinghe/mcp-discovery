import sys
from pathlib import Path

# Make the in-repo package importable when running `pytest agent/mcp/tests`
# directly (without installing mcp-discovery).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
