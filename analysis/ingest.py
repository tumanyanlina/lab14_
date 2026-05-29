import glob
import sys
from pathlib import Path

import polars as pl


DATA_DIR = Path(__file__).parent.parent / "data"
PARQUET_PATH = DATA_DIR / "mfc_clean.parquet"


def load_ndjson(data_dir: Path) -> pl.DataFrame:
    """Read all NDJSON files from data_dir into a single Polars DataFrame."""
    files = sorted(glob.glob(str(data_dir / "*.ndjson")))
    if not files:
        print(f"[ERROR] no .ndjson files found in {data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] loading {len(files)} file(s) from {data_dir}")
    frames = [pl.read_ndjson(f) for f in files]
    df = pl.concat(frames)
    print(f"[INFO] loaded {df.height} rows, {df.width} columns")
    return df


def show_overview(df: pl.DataFrame) -> None:
    """Print first 5 rows and schema overview."""
    print("\n--- первые 5 строк ---")
    print(df.head(5))
    print("\n--- схема ---")
    print(df.schema)
    print(f"\nВсего строк : {df.height}")
    print(f"Всего колонок: {df.width}")
    print("\n--- пропуски по колонкам ---")
    print(df.null_count())


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """
    Задание 5 — очистка и валидация:
    1. Привести типы (timestamp → Datetime).
    2. Убрать дубликаты по (window_id, window_start).
    3. Удалить строки с отрицательными значениями.
    4. Заполнить пропуски медианой.
    """
    print("\n[STEP] приведение типов...")
    df = df.with_columns([
        pl.col("window_start").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("window_end").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("service_type").cast(pl.Categorical),
    ])

    before = df.height
    print(f"[STEP] удаление дубликатов (было {before})...")
    df = df.unique(subset=["window_id", "window_start"])
    print(f"       осталось {df.height} (удалено {before - df.height})")

    print("[STEP] фильтрация невалидных значений...")
    df = df.filter(
        (pl.col("avg_queue_length") >= 0) &
        (pl.col("avg_wait_time_sec") >= 0) &
        (pl.col("sample_count") > 0)
    )
    print(f"       осталось {df.height}")

    numeric_cols = [
        "avg_queue_length", "max_queue_length", "min_queue_length",
        "avg_wait_time_sec", "max_wait_time_sec",
    ]
    print("[STEP] заполнение пропусков медианой...")
    for col in numeric_cols:
        median_val = df[col].median()
        df = df.with_columns(pl.col(col).fill_null(median_val))

    print(f"[INFO] очистка завершена: {df.height} строк")
    return df


def main() -> None:
    df_raw = load_ndjson(DATA_DIR)
    show_overview(df_raw)
    df_clean = clean(df_raw)

    DATA_DIR.mkdir(exist_ok=True)
    df_clean.write_parquet(PARQUET_PATH)
    print(f"\n[INFO] данные сохранены в {PARQUET_PATH}")

    print("\n--- очищенные данные (первые 5 строк) ---")
    print(df_clean.head(5))


if __name__ == "__main__":
    main()