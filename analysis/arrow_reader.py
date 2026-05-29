import glob
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import polars as pl


DATA_DIR = Path(__file__).parent.parent / "data"


def read_arrow_files() -> pl.DataFrame:
    """Read all .arrow files into a single Polars DataFrame."""
    files = sorted(glob.glob(str(DATA_DIR / "*.arrow")))
    if not files:
        raise FileNotFoundError(f"No .arrow files found in {DATA_DIR}")

    print(f"[INFO] найдено {len(files)} .arrow файлов")

    tables = []
    for f in files:
        with pa.memory_map(f, "r") as source:
            reader = ipc.open_file(source)
            tables.append(reader.read_all())

    combined = pa.concat_tables(tables)
    df = pl.from_arrow(combined)
    print(f"[INFO] загружено {df.height} строк из Arrow")
    return df


def read_ndjson_files() -> pl.DataFrame:
    """Read all .ndjson files into a single Polars DataFrame."""
    files = sorted(glob.glob(str(DATA_DIR / "*.ndjson")))
    if not files:
        raise FileNotFoundError(f"No .ndjson files found in {DATA_DIR}")

    print(f"[INFO] найдено {len(files)} .ndjson файлов")
    frames = [pl.read_ndjson(f) for f in files]
    df = pl.concat(frames)
    print(f"[INFO] загружено {df.height} строк из NDJSON")
    return df


def compare_formats() -> None:
    """Compare Arrow vs NDJSON: file size and read speed."""
    print("\n=== Сравнение форматов: Arrow vs NDJSON ===")

    # File sizes.
    arrow_files = glob.glob(str(DATA_DIR / "*.arrow"))
    ndjson_files = glob.glob(str(DATA_DIR / "*.ndjson"))

    arrow_size = sum(Path(f).stat().st_size for f in arrow_files)
    ndjson_size = sum(Path(f).stat().st_size for f in ndjson_files)

    print(f"\nРазмер файлов:")
    print(f"  Arrow  : {arrow_size:,} байт  ({len(arrow_files)} файлов)")
    print(f"  NDJSON : {ndjson_size:,} байт  ({len(ndjson_files)} файлов)")
    if ndjson_size > 0:
        ratio = ndjson_size / arrow_size if arrow_size > 0 else 0
        print(f"  NDJSON / Arrow = {ratio:.2f}x")

    # Read speed.
    print(f"\nСкорость чтения:")

    t0 = time.perf_counter()
    df_arrow = read_arrow_files()
    arrow_time = time.perf_counter() - t0
    print(f"  Arrow  : {arrow_time * 1000:.2f} ms ({df_arrow.height} строк)")

    t0 = time.perf_counter()
    df_ndjson = read_ndjson_files()
    ndjson_time = time.perf_counter() - t0
    print(f"  NDJSON : {ndjson_time * 1000:.2f} ms ({df_ndjson.height} строк)")

    faster = "Arrow" if arrow_time < ndjson_time else "NDJSON"
    ratio = max(arrow_time, ndjson_time) / min(arrow_time, ndjson_time)
    print(f"\n  Быстрее: {faster} (в {ratio:.1f}x)")

    print("\n--- Arrow DataFrame (первые 5 строк) ---")
    print(df_arrow.head(5))


def main() -> None:
    compare_formats()


if __name__ == "__main__":
    main()