import mfc_validator

# Тест 1: валидная запись
ok, err = mfc_validator.validate_queue_record(
    window_id=1,
    avg_queue_length=4.5,
    max_queue_length=7.0,
    min_queue_length=2.0,
    avg_wait_time_sec=1080.0,
    sample_count=15,
    service_type="passport",
)
print(f"Тест 1 (валидная): ok={ok}, err='{err}'")

# Тест 2: отрицательная очередь
ok, err = mfc_validator.validate_queue_record(
    window_id=1,
    avg_queue_length=-1.0,
    max_queue_length=7.0,
    min_queue_length=-2.0,
    avg_wait_time_sec=1080.0,
    sample_count=15,
    service_type="passport",
)
print(f"Тест 2 (негативная очередь): ok={ok}, err='{err}'")

# Тест 3: неизвестный тип услуги
ok, err = mfc_validator.validate_queue_record(
    window_id=1,
    avg_queue_length=4.5,
    max_queue_length=7.0,
    min_queue_length=2.0,
    avg_wait_time_sec=1080.0,
    sample_count=15,
    service_type="unknown_service",
)
print(f"Тест 3 (неизвестный сервис): ok={ok}, err='{err}'")

# Тест 4: avg > max
ok, err = mfc_validator.validate_queue_record(
    window_id=1,
    avg_queue_length=10.0,
    max_queue_length=7.0,
    min_queue_length=2.0,
    avg_wait_time_sec=1080.0,
    sample_count=15,
    service_type="tax",
)
print(f"Тест 4 (avg > max): ok={ok}, err='{err}'")

# Тест 5: батч валидация
batch = [
    (1, 4.5, 7.0, 2.0, 1080.0, 15, "passport"),
    (2, -1.0, 5.0, -2.0, 500.0, 10, "tax"),
    (3, 3.0, 6.0, 1.0, 720.0, 12, "unknown"),
]
errors = mfc_validator.validate_batch(batch)
print(f"\nТест 5 (батч из 3, ошибок={len(errors)}):")
for idx, msg in errors:
    print(f"  строка {idx}: {msg}")