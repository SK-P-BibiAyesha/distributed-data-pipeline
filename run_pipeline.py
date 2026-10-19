
import subprocess
import sys

steps = [
    ("Data ingestion", "src/ingest.py"),
    ("Data transformation", "src/transform.py"),
    ("Data cleaning", "src/clean.py"),
    ("Sales analytics", "src/analytics.py"),
]

for name, script in steps:
    print(f"\n{'=' * 50}")
    print(f"Running: {name}")
    print(f"{'=' * 50}", flush=True)

    subprocess.run(
        [sys.executable, script],
        check=True,
    )

print("\nPipeline completed successfully!")
