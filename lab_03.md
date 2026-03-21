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

<img width="365" height="920" alt="image" src="https://github.com/user-attachments/assets/c7c20ff4-a484-4132-a649-153dac095686" />  


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

## Подготовка среды

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
```
<img width="714" height="287" alt="image" src="https://github.com/user-attachments/assets/46e24d13-edfe-461b-abcd-e7683dc791e0" />  
<img width="882" height="483" alt="image" src="https://github.com/user-attachments/assets/69fbaa2c-1929-43d6-9c44-0231b1f40f58" />  


**Данные таблицы trips (17 записей):**
<img width="742" height="402" alt="image" src="https://github.com/user-attachments/assets/7a8cd569-3ad7-4a09-bfb2-852cc46683c9" />  

### 2. Файловые источники
**fuel_costs.csv (затраты на топливо):**  
<img width="712" height="200" alt="image" src="https://github.com/user-attachments/assets/b0de0117-3480-4fee-9790-003bac5dcc48" />  

**route_sheets.csv (маршрутные листы):**  
<img width="590" height="539" alt="image" src="https://github.com/user-attachments/assets/215b5f42-2f2f-45dd-b4e1-d98efb5fd1b5" />  

**Запуск PostgreSQL контейнера:**  
<img width="447" height="305" alt="image" src="https://github.com/user-attachments/assets/91375218-8c57-45e8-9c88-0e71d73c44c6" />  


### 3. MySQL (Целевое хранилище)  

**Создание таблицы fact_logistics_report:**  
Создание таблицы fact_logistics_report:
```sql
CREATE TABLE fact_logistics_report (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id INT,
    vehicle_id VARCHAR(20),
    driver_name VARCHAR(100),
    route VARCHAR(200),
    departure_date DATE,
    return_date DATE,
    fuel_type VARCHAR(20),
    fuel_cost_per_liter DECIMAL(10,2),
    fuel_consumption_per_100km DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    cargo_weight_kg INT,
    additional_expenses DECIMAL(10,2),
    total_fuel_cost DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    cost_per_km DECIMAL(10,2),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Создание представления view_logistics_analytics:**  
```sql
CREATE OR REPLACE VIEW view_logistics_analytics AS
SELECT 
    trip_id,
    vehicle_id,
    driver_name,
    route,
    departure_date,
    distance_km,
    fuel_type,
    fuel_consumption_per_100km,
    total_fuel_cost,
    additional_expenses,
    total_cost,
    cost_per_km,
    CASE 
        WHEN cost_per_km < 15 THEN 'Низкая стоимость'
        WHEN cost_per_km BETWEEN 15 AND 25 THEN 'Средняя стоимость'
        ELSE 'Высокая стоимость'
    END AS cost_category
FROM fact_logistics_report
ORDER BY cost_per_km DESC;
```

### ETL Реализация (Pentaho Data Integration)
**Настройка подключений в Spoon**  
**Трансформация lab_03.ktr:**  

<img width="1227" height="699" alt="image" src="https://github.com/user-attachments/assets/0312a0d4-c038-48ba-a011-e6e1ec3aae99" />  

**Шаги трансформации:**  
-Read trips from PostgreSQL Table Input	Чтение данных о рейсах из PostgreSQL  
<img width="1054" height="430" alt="image" src="https://github.com/user-attachments/assets/c0d90b94-6c9c-4962-9b67-abf8523d8b95" />  
-Read fuel costs from CSV	CSV Input Чтение затрат на топливо  
<img width="952" height="625" alt="image" src="https://github.com/user-attachments/assets/46c74fd1-d1fc-44e1-93e4-9d947bec125a" />  
-Read route sheets from CSV	CSV Input	Чтение маршрутных листов  
<img width="902" height="647" alt="image" src="https://github.com/user-attachments/assets/46239653-6e45-4773-82ee-82c78fdac5cd" />  
-Join trips with fuel costs	Merge Join	Объединение по vehicle_id (INNER JOIN)  
<img width="469" height="439" alt="image" src="https://github.com/user-attachments/assets/7e973252-5d70-4abc-b896-1f4bf214c4ed" />  
-Join with route sheets	Merge Join	Объединение по trip_id (INNER JOIN)  
<img width="454" height="420" alt="image" src="https://github.com/user-attachments/assets/96e3105b-f124-4407-821f-e307f8ebfb3d" />  
-Calculate cost per km	Modified JavaScript Value	Расчет total_fuel_cost, total_cost, cost_per_km  
<img width="977" height="612" alt="image" src="https://github.com/user-attachments/assets/0e30e794-9bbe-44b3-a450-57b26411402c" />  
-Load to MySQL	Table Output	Загрузка данных в MySQL  
<img width="950" height="433" alt="image" src="https://github.com/user-attachments/assets/049fa8e2-ff0c-47bc-ab5e-ee868bafc389" />  

Код для расчетов:
```bash
var dist = distance_km;
var consumption = fuel_consumption_per_100km;
var cost_liter = fuel_cost_per_liter;
var expenses = additional_expenses;

var total_fuel_cost = (dist / 100) * consumption * cost_liter;
var total_cost = total_fuel_cost + expenses;
var cost_per_km = total_cost / dist;

var total_fuel = total_fuel_cost;
var total_cost_val = total_cost;
var cost_per_km_val = cost_per_km;

true;
```

**Логи выполнения:**  
<img width="910" height="229" alt="image" src="https://github.com/user-attachments/assets/10a838bd-6489-4eb4-8ed3-768f35fddf71" />  

### Результаты в MySQL
**Таблица fact_logistics_report**  
```sql
SELECT trip_id, vehicle_id, distance_km, total_cost, cost_per_km 
FROM fact_logistics_report 
ORDER BY trip_id;
```

<img width="2474" height="1486" alt="image" src="https://github.com/user-attachments/assets/dd2e5ac9-fa39-426a-ab53-2069ecadca84" />  

**Представление view_logistics_analytics**  
```sql
SELECT trip_id, vehicle_id, cost_per_km, cost_category 
FROM view_logistics_analytics 
ORDER BY cost_per_km DESC;
```

<img width="2400" height="1332" alt="image" src="https://github.com/user-attachments/assets/2b2eeefc-412d-4ac8-a348-68c2f54539f6" />  

## Анализ результатов
### Выводы  
Все рейсы имеют низкую стоимость (< 15 руб/км):  
Самый экономичный рейс: trip_id 14 (G707) - 3.18 руб/км  
Самый затратный рейс: trip_id 6 (D404) - 8.42 руб/км  
**Факторы, влияющие на стоимость:**  
Электромобиль (G707) показал наименьшую стоимость  
Рейсы с большим расстоянием имеют более высокую общую стоимость, но удельная стоимость остается низкой  
**Качество данных:**  
Все 17 рейсов из PostgreSQL успешно объединены с файловыми источниками  
7 уникальных транспортных средств с полными данными о топливе  
Отсутствие пропусков в ключевых полях  











