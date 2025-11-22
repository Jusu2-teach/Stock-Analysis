#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AStock Pipeline - Main Entry Point
=================================

Bootstraps the environment and delegates to the CLI.
"""

import sys
from pathlib import Path

# --- Robust path/bootstrap handling ---
THIS_FILE = Path(__file__)
PIPELINE_DIR = THIS_FILE.parent               # .../pipeline
PROJECT_ROOT = PIPELINE_DIR.parent            # project root
SRC_DIR = PROJECT_ROOT / 'src'

def _ensure_path(p: Path):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

# Ensure project root and src are in sys.path
_ensure_path(PROJECT_ROOT)
if SRC_DIR.exists():
    _ensure_path(SRC_DIR)

if __name__ == "__main__":
    try:
        from pipeline.cli import main
        main()
    except ImportError as e:
        print(f"[FATAL] Failed to import pipeline CLI: {e}")
        print(f"sys.path: {sys.path}")
        sys.exit(1)
