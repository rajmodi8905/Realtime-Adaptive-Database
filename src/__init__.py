# ==============================================
# Adaptive Database Framework
# ==============================================
#
# Package Structure (4 Topics + Orchestrator):
#
# src/
# ├── normalization/    # Topic 1: Normalize raw ingested data
# ├── analysis/         # Topic 2: Analyze & classify fields
# ├── storage/          # Topic 3: Insert into MySQL / MongoDB
# ├── persistence/      # Topic 4: Metadata persistence across restarts
# ├── config.py         # Configuration management
# ├── ingest_and_classify.py  # Final orchestrator class
# └── cli.py            # Command line entry point
#
# ==============================================

__version__ = "0.1.0"
