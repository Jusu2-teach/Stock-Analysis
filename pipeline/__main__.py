"""
Pipeline v2 CLI entry point.

Usage:
    python -m pipeline run -c workflow/analysis.yaml
    python -m pipeline graph -c workflow/analysis.yaml
    python -m pipeline validate -c workflow/analysis.yaml
    python -m pipeline engines
"""

from pipeline.cli.main import main

if __name__ == "__main__":
    main()
