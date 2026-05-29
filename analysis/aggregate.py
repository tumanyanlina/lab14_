import time
from pathlib import Path

import duckdb
import polars as pl


PARQUET_PATH = Path(__file__).parent.parent / "data" / "mfc_clean.parquet"


def load_parquet() -> pl.DataFrame:
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"{PARQUET_PATH} not found. Run ingest.py first."
        )
    return pl.read_parquet(PARQUET_PATH)


def analyze_by_service(df: pl.DataFrame) -> pl.DataFrame:
    """Группировка по типу услуги: среднее, макс, мин очереди."""
    return (
        df.group_by("service_type")
        .agg([
            pl.col("avg_queue_length").mean().alias("avg_queue"),
            pl.col("max_queue_length").max().alias("max_queue"),
            pl.col("min_queue_length").min().alias("min_queue"),
            pl.col("avg_wait_time_sec").mean().alias("avg_wait_sec"),
            pl.col("sample_count").sum().alias("total_samples"),
            pl.col("window_id").n_unique().alias("active_windows"),
        ])
        .sort("avg_queue", descending=True)
    )


def analyze_by_window(df: pl.DataFrame) -> pl.DataFrame:
    """Группировка по окошку: средняя нагрузка и время ожидания."""
    return (
        df.group_by("window_id")
        .agg([
            pl.col("avg_queue_length").mean().alias("avg_queue"),
            pl.col("max_queue_length").max().alias("peak_queue"),
            pl.col("avg_wait_time_sec").mean().alias("avg_wait_sec"),
            pl.col("sample_count").sum().alias("total_samples"),
        ])
        .sort("window_id")
    )


def analyze_by_hour(df: pl.DataFrame) -> pl.DataFrame:
    """Нагрузка по часу дня из window_start."""
    return (
        df.with_columns(pl.col("window_start").dt.hour().alias("hour"))
        .group_by("hour")
        .agg([
            pl.col("avg_queue_length").mean().alias("avg_queue"),
            pl.col("avg_wait_time_sec").mean().alias("avg_wait_sec"),
            pl.col("sample_count").sum().alias("total_samples"),
        ])
        .sort("hour")
    )

def run_duckdb() -> pl.DataFrame:
    query = f"""
        SELECT
            service_type,
            ROUND(AVG(avg_queue_length), 2)  AS avg_queue,
            MAX(max_queue_length)            AS max_queue,
            MIN(min_queue_length)            AS min_queue,
            ROUND(AVG(avg_wait_time_sec), 1) AS avg_wait_sec,
            SUM(sample_count)                AS total_samples,
            COUNT(DISTINCT window_id)        AS active_windows
        FROM '{PARQUET_PATH}'
        GROUP BY service_type
        ORDER BY avg_queue DESC
    """
    conn = duckdb.connect()
    result = conn.execute(query).pl()
    conn.close()
    return result


def benchmark(df: pl.DataFrame) -> None:
    """Сравнение времени выполнения: Polars vs DuckDB."""
    print("\n=== Сравнение производительности ===")

    t0 = time.perf_counter()
    polars_result = analyze_by_service(df)
    polars_time = time.perf_counter() - t0
    print(f"Polars  : {polars_time * 1000:.2f} ms  ({len(polars_result)} строк)")

    t0 = time.perf_counter()
    duck_result = run_duckdb()
    duck_time = time.perf_counter() - t0
    print(f"DuckDB  : {duck_time * 1000:.2f} ms  ({len(duck_result)} строк)")

    faster = "Polars" if polars_time < duck_time else "DuckDB"
    ratio = max(polars_time, duck_time) / min(polars_time, duck_time)
    print(f"Быстрее : {faster} (в {ratio:.1f}x)")


def main() -> None:
    df = load_parquet()
    print(f"[INFO] загружено {df.height} строк из Parquet")

    print("\n=== По типу услуги (Polars) ===")
    by_service = analyze_by_service(df)
    print(by_service)

    print("\n=== По окошку (Polars) ===")
    by_window = analyze_by_window(df)
    print(by_window)

    print("\n=== По часу дня (Polars) ===")
    by_hour = analyze_by_hour(df)
    print(by_hour)

    print("\n=== DuckDB SQL-результат ===")
    duck = run_duckdb()
    print(duck)

    benchmark(df)

    out_dir = Path(__file__).parent.parent / "data"
    by_service.write_parquet(out_dir / "agg_by_service.parquet")
    by_window.write_parquet(out_dir / "agg_by_window.parquet")
    by_hour.write_parquet(out_dir / "agg_by_hour.parquet")
    print("\n[INFO] агрегаты сохранены в data/")


if __name__ == "__main__":
    main()