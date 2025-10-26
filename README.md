
## ЗАПУСК

```bash
docker compose up
```

---

## ТЕСТУВАННЯ

> У цьому рішенні реалізовано спрощений підхід для тестування у межах технічного завдання

1. У контейнері бази даних (psql) створити `test_db`
2. На хості запустити:

```bash
pytest -v
```

---

## МЕТРИКИ / БЕНЧМАРК / ЛОГУВАННЯ

* **Middleware:** обмеження частоти запитів (rate limiting) для унікальних клієнтів

  ```python
  RATE_LIMIT = 100
  ```

* **POST endpoint:** логування (файл + консоль), метрика — кількість подій через простий лічильник

* **Скрипт для історичних даних:** метрики

  * Оброблені рядки
  * Вставлені рядки
  * Пропущені рядки (невалідні або дублікати)
  * Швидкість вставки (rows/sec)

* **GET endpoints:** логування (файл + консоль)

* **Celery worker:** логування

* **Benchmark:** `benchmark.py` — тестує `POST` та `GET` endpoint’и

  * Для `POST` використовується параметр `dry_run`, щоб не записувати дані в базу

---

## Приклади результатів

**Скрипт для історичних даних:**

```
Processed rows: 5000
Inserted rows: 5000
Skipped rows (invalid or duplicates): 0
```

**Benchmark.py:**

```
Sent 10000 events in 0.22s, speed=45794 events/sec
DAU: [{'day': '2025-10-24', 'dau': 2}]
Top events: [{'event_type': 'login', 'count': 16}, {'event_type': 'click', 'count': 8}]
Retention: {'start_date': '2025-10-19', 'retention': [{'day': 1, 'returning_users': 0}, ...]}
```


