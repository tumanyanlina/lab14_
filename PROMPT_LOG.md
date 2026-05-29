Лабораторная работа №14

Студент: Туманян Лина Врежовна
Группа: 220032-11
Сложность: повышенная
ИИ-инструмент: Claude Sonnet 4.6 (claude.ai)

Шаг 1 — Инициализация проекта: .gitignore

Промпт: Создай .gitignore для проекта на Go и Python (Polars, DuckDB, Streamlit). Исключить: бинарники Go, __pycache__, .venv, сгенерированные data/*.ndjson, data/*.parquet, папку analysis/plots/, кэш Streamlit, папки IDE (.vscode, .idea),системные файлы ОС.

Результат: Создан файл .gitignore с секциями Go, Python, Data files,
Plots, Streamlit cache, IDE, OS.

.gitignore:

# Go
collector/collector.exe
collector/*.exe

# Python
__pycache__/
*.pyc
.venv/
venv/

# Data files (generated)
data/*.ndjson
data/*.parquet

# Plots (generated)
analysis/plots/

# Streamlit cache
.streamlit/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db