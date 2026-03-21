# Лабораторная работа 3.1 Интеграция данных из нескольких источников. Обработка и согласование данных из разных источников  

**Вариант 9: Логистика**

---

## Описание задачи

Разработать комплексное ETL-решение для интеграции данных из трех источников:
- **PostgreSQL** (таблица `trips` с данными о рейсах)
- **Excel/CSV** (справочник затрат на топливо `fuel_costs.csv`)
- **CSV** (маршрутные листы `route_sheets.csv`)

Целевое хранилище: **MySQL** (таблица `fact_logistics_report`)  

### Бизнес-задача
Рассчитать стоимость 1 км пробега для каждого рейса и классифицировать затраты по категориям.

### Формулы расчета  
```bash
total_fuel_cost = (distance_km / 100) × fuel_consumption_per_100km × fuel_cost_per_liter
total_cost = total_fuel_cost + additional_expenses
cost_per_km = total_cost / distance_km
```

---

## Архитектура решения

Схема архитектуры включает три обязательных слоя:

![Архитектура ETL решения](architecture/architecture.png)

### Source Layer (Слой источников)
- **PostgreSQL**: Таблица `trips` с данными о 17 рейсах
- **CSV файл**: `fuel_costs.csv` - затраты на топливо (7 записей)
- **CSV файл**: `route_sheets.csv` - маршрутные листы (17 записей)

### Storage Layer (Слой хранения)
- **Staging Area**: Промежуточная обработка в Pentaho
- **fact_logistics_report**: Таблица фактов с расчетными показателями
- **view_logistics_analytics**: Представление для бизнес-аналитики

### Business Layer (Бизнес-слой)
- Аналитический отчет: стоимость 1 км пробега с категоризацией затрат

---

## 🛠️ Подготовка среды

### 1. PostgreSQL (Источник данных)

**Создание таблицы `trips`:**

```sql
DROP TABLE IF EXISTS trips CASCADE;

CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    driver_name VARCHAR(100),
    route VARCHAR(200),
    departure_date DATE,
    return_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
