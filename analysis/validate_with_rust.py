"""
validate_with_rust.py — П.4: Интеграция Rust-библиотеки для валидации
Использует mfc_validator (Rust/PyO3) для валидации данных из Parquet.
"""

import sys
from pathlib import Path

import polars as pl

# Добавляем путь к Rust-библиотеке.
sys.path.insert(0, str(Path(__file__).parent.parent / "validator" / ".venv" / "Lib" / "site-packages"))
import mfc_validator


DATA_DIR = Path(__file__).parent.parent / "data"
PARQUET_PATH = DATA_DIR / "mfc_clean.parquet"


def validate_dataframe(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Validate all records using Rust mfc_validator.
    Returns (valid_df, invalid_df).
    """
    records = [
        (
            int(row["window_id"]),
            float(row["avg_queue_length"]),
            float(row["max_queue_length"]),
            float(row["min_queue_length"]),
            float(row["avg_wait_time_sec"]),
            int(row["sample_count"]),
            str(row["service_type"]),
        )
        for row in df.iter_rows(named=True)
    ]

    errors = mfc_validator.validate_batch(records)
    error_indices = {idx for idx, _ in errors}

    valid_mask = [i not in error_indices for i in range(len(records))]
    invalid_mask = [i in error_indices for i in range(len(records))]

    valid_df = df.filter(pl.Series(valid_mask))
    invalid_df = df.filter(pl.Series(invalid_mask))

    return valid_df, invalid_df


def main() -> None:
    print("[INFO] загружаем данные из Parquet...")
    df = pl.read_parquet(PARQUET_PATH)
    print(f"[INFO] загружено {df.height} строк")

    print("\n[INFO] валидация через Rust (mfc_validator)...")
    valid_df, invalid_df = validate_dataframe(df)

    print(f"\n=== Результаты валидации ===")
    print(f"  Всего записей : {df.height}")
    print(f"  Валидных      : {valid_df.height}")
    print(f"  Невалидных    : {invalid_df.height}")

    if invalid_df.height > 0:
        print("\n--- Невалидные записи ---")
        print(invalid_df)
    else:
        print("\n✓ Все записи прошли валидацию!")

    print("\n--- Валидные записи (первые 5) ---")
    print(valid_df.head(5))


if __name__ == "__main__":
    main()