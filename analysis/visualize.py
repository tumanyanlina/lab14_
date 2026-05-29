from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import polars as pl


DATA_DIR = Path(__file__).parent.parent / "data"
PLOTS_DIR = Path(__file__).parent / "plots"
PLOTS_DIR.mkdir(exist_ok=True)


def load_clean() -> pl.DataFrame:
    path = DATA_DIR / "mfc_clean.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run ingest.py first.")
    return pl.read_parquet(path)


def plot_time_series(df: pl.DataFrame) -> None:
    """График 1: динамика средней длины очереди по окошкам."""
    df = df.with_columns(
        pl.col("window_start").dt.to_string("%H:%M:%S").alias("time_str")
    )

    fig = px.line(
        df.to_pandas(),
        x="time_str",
        y="avg_queue_length",
        color="window_id",
        title="Динамика длины очереди по окошкам МФЦ",
        labels={
            "time_str": "Время",
            "avg_queue_length": "Средняя длина очереди (чел.)",
            "window_id": "Окошко №",
        },
        template="plotly_white",
    )
    fig.update_layout(
        hovermode="x unified",
        legend_title_text="Окошко",
        font=dict(size=13),
    )
    fig.write_html(str(PLOTS_DIR / "01_time_series.html"))
    fig.write_image(str(PLOTS_DIR / "01_time_series.png"), width=1200, height=500)
    print("[INFO] сохранён: plots/01_time_series.png")


def plot_heatmap(df: pl.DataFrame) -> None:
    """График 2: тепловая карта нагрузки (окошко × час дня)."""
    df = df.with_columns(pl.col("window_start").dt.hour().alias("hour"))

    pivot = (
        df.group_by(["window_id", "hour"])
        .agg(pl.col("avg_queue_length").mean().alias("avg_queue"))
        .sort(["window_id", "hour"])
        .pivot(on="hour", index="window_id", values="avg_queue")
        .sort("window_id")
    )

    hour_cols = [c for c in pivot.columns if c != "window_id"]
    z = pivot.select(hour_cols).to_numpy()
    x = [f"{h}:00" for h in hour_cols]
    y = [f"Окошко {wid}" for wid in pivot["window_id"].to_list()]

    fig = go.Figure(go.Heatmap(
        z=z,
        x=x,
        y=y,
        colorscale="YlOrRd",
        colorbar=dict(title="Ср. очередь"),
    ))
    fig.update_layout(
        title="Тепловая карта нагрузки МФЦ (окошко × час дня)",
        xaxis_title="Час дня",
        yaxis_title="Окошко",
        template="plotly_white",
        font=dict(size=13),
    )
    fig.write_html(str(PLOTS_DIR / "02_heatmap.html"))
    fig.write_image(str(PLOTS_DIR / "02_heatmap.png"), width=1100, height=500)
    print("[INFO] сохранён: plots/02_heatmap.png")


def plot_wait_distribution(df: pl.DataFrame) -> None:
    """График 3: распределение времени ожидания по типам услуг."""
    df = df.with_columns(
        (pl.col("avg_wait_time_sec") / 60).alias("avg_wait_min")
    )

    fig = px.box(
        df.to_pandas(),
        x="service_type",
        y="avg_wait_min",
        color="service_type",
        title="Распределение времени ожидания по типам услуг МФЦ",
        labels={
            "service_type": "Тип услуги",
            "avg_wait_min": "Среднее время ожидания (мин.)",
        },
        template="plotly_white",
    )
    fig.update_layout(showlegend=False, font=dict(size=13))
    fig.write_html(str(PLOTS_DIR / "03_wait_distribution.html"))
    fig.write_image(str(PLOTS_DIR / "03_wait_distribution.png"), width=1000, height=500)
    print("[INFO] сохранён: plots/03_wait_distribution.png")


def main() -> None:
    print("[INFO] загружаем данные...")
    df = load_clean()
    print(f"[INFO] строк: {df.height}")

    print("[INFO] строим графики...")
    plot_time_series(df)
    plot_heatmap(df)
    plot_wait_distribution(df)

    print(f"\n[INFO] все графики сохранены в {PLOTS_DIR}")


if __name__ == "__main__":
    main()