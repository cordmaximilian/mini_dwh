"""Data source fetcher modules for the demo warehouse."""

# Modules are discovered via their ``fetch`` functions referenced in
# ``pipeline_config.yml``.

from pathlib import Path

# Raw CSV files are stored outside the repository in a sibling directory.
# ``EXTERNAL_DATA_DIR`` resolves to ``../external_data`` relative to the
# repository root.
EXTERNAL_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "external_data"
