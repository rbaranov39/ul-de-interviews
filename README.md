# Тестовое задание «Умная логистика»

## Запуск

1. Создайте локальный файл настроек:

```bash
cp .env.example .env
```

2. Сгенерируйте два разных ключа и запишите их в `AIRFLOW_JWT_SECRET` и `SUPERSET_SECRET_KEY` файла `.env`:

```bash
openssl rand -base64 42
openssl rand -base64 42
```

3. Проверьте конфигурацию и поднимите стенд:

```bash
make config
make build
make up
make status
```

`airflow-init` и `superset-init` должны завершиться со статусом `Exited (0)`. При первом запуске PostgreSQL создает источник с 300 000 цен и пустые таблицы DWH.

4. Откройте Airflow и вручную запустите `dwh_pipeline` без параметров. DAG выполнит полный backfill, DQ-проверки, построит витрину и опубликует ее в ClickHouse.

5. После успешного `dwh_pipeline` запустите `ml_pipeline`. Он обучит CatBoost, рассчитает прогнозы для test и future и запишет их в `dm.ml_price_predictions`.

6. Откройте Superset и выберите дашборд `Market Rate Corridor`.

Локальные адреса:

| Сервис | Адрес |
| --- | --- |
| Airflow | <http://localhost:8080> |
| Superset | <http://localhost:18088> |
| PostgreSQL | `localhost:5454` |
| ClickHouse HTTP | `localhost:18123` |
| ClickHouse Native | `localhost:19000` |

Логин и пароль локальных UI: `user` / `password`.

Контрольные значения после полного `dwh_pipeline`:

```text
source prices       300 000
STG current         300 000
ODS valid           299 797
DQ quarantine           203
market corridor     203 644
ClickHouse rows      203 644
ClickHouse parts          37
```

## Архитектура

```text
PostgreSQL public
    -> STG
    -> ODS + DQ quarantine
    -> DDS
    -> DM
    -> ClickHouse
    -> Superset
```

Служебные базы Airflow и Superset работают в отдельных экземплярах PostgreSQL.

### Слои данных

- `stg.prices` хранит версии исходных цен. Текущая версия имеет `_superseded_at IS NULL`.
- `stg.cities`, `stg.exchange_rates` и `stg.body_types` загружаются полным снимком.
- `ods.prices` содержит текущие валидные цены в рублях.
- `ods.dq_quarantine_prices` хранит невалидные цены и массив кодов причин.
- `dds.dim_city` содержит по одной записи на `gc_id`.
- `dds.dim_body_group` и bridge-таблицы нормализуют кузова и месяцы действия цены.
- `dm.mart_market_corridor` содержит рыночную витрину.
- `dm.ml_price_predictions` содержит факт, прогноз и ошибки ML-модели.

Исправленная строка источника получает новую версию STG, повторно проходит проверки и удаляется из карантина при успешной валидации.

## Рыночная витрина

Зерно `dm.mart_market_corridor`:

```text
месяц
x город отправления
x город назначения
x группа кузова
x весовая категория
```

В расчет входят фактические простые перевозки с положительной ценой и дистанцией, `outlier_mark = false` и датой рынка не позднее `2026-10-26`.

Метрики:

- `p25_trip`, `median_trip`, `p75_trip`, `avg_trip` - стоимость рейса в рублях;
- `p25_km`, `median_km`, `p75_km` - стоимость километра;
- `trips_count`, `carriers_count`, `cv_rate` - объем и устойчивость выборки;
- `mom_pct`, `yoy_pct` - изменение медианы;
- `reliability_level` - `HIGH`, `MEDIUM` или `LOW_DATA`.

После проверки витрины Airflow читает ее из PostgreSQL пакетами по 10 000 строк и записывает через `clickhouse-connect` в `analytics.market_corridor_stage`. Количество строк, уникальность зерна и `SUM(trips_count)` проверяются до замены месячных партиций основной таблицы `analytics.market_corridor`.

Superset читает данные из ClickHouse. Дашборд содержит график цены, количество перевозок и детальную таблицу. Фильтры по месяцу, городам, кузову и весовой категории применяются ко всем трем элементам.

## Качество данных

Цена попадает в карантин при любом из условий:

- `price <= 0`;
- цена в RUB не превышает 1000;
- `distance <= 0`;
- отрицательный вес или объем;
- город отсутствует в справочнике;
- для иностранной валюты нет курса на `nearest_rate_date`.

Для RUB используется курс `1.0`. USD и EUR переводятся в рубли только по точному совпадению даты курса.

Пустые категории веса и объема рассчитываются по правилам задания. Исходный `outlier_mark` сохраняется в ODS, но отмеченные строки не попадают в рыночную витрину.

Повторный запуск за тот же интервал не создает новые версии неизменившихся цен. ODS использует `ON CONFLICT`, связи пересобираются через `DELETE + INSERT`, а ClickHouse получает данные через stage-таблицу и замену партиций.

## ML

Модель `CatBoostRegressor` предсказывает полную стоимость конкретного рейса в рублях - `price_rub`.

Признаки:

- города, регионы и координаты;
- расстояние, вес и объем;
- группа кузова и весогабаритные категории;
- год и циклические признаки даты погрузки.

Цена, ставка за километр, `price_group_stats` и `outlier_mark` в признаки не входят. Данные разделяются по времени:

```text
train   [2026-07-26, 2026-10-05)
test    [2026-10-05, 2026-10-26]
future  loading_date > 2026-10-26
```

Для test сохраняются факт, прогноз и ошибка. Для future сохраняется только `predicted_price_rub`.

Проверенные метрики test-периода:

```text
R2        0.9182
MAPE      16.02%
RMSE      54 717 руб.
MAE       28 530 руб.
```

## Дополнительные команды

Интервальный ETL использует полуинтервал `[start_date, end_date)`:

```bash
docker compose exec -T airflow-scheduler \
  python /opt/airflow/etl/load_stg.py 2026-10-05 2026-10-06
```

Остановить стенд с сохранением данных:

```bash
make down
```

Удалить контейнеры и volumes проекта:

```bash
make destroy
```
