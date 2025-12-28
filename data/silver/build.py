"""
build.py

Silver build entrypoint.
"""
from pathlib import Path
from data.silver.sec_silver import build_sec_silver
from data.silver.stooq_silver import build_stooq_silver


def main():
  bronze = Path("data/bronze")
  silver = Path("data/silver")
  silver.mkdir(parents=True, exist_ok=True)

  build_sec_silver(bronze_dir=bronze, silver_dir=silver)
  build_stooq_silver(bronze_dir=bronze, silver_dir=silver)


if __name__ == "__main__":
  main()
