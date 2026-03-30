## Отчет по лабораторной работе №5. Проектирование объектной модели данных. Проектирование сквозного конвейера ETL  
### Вариант 9: Прогноз погоды для Афин
---
## Цель работы
Развернуть среду оркестрации Apache Airflow с использованием Docker.  
Изучить структуру и принципы работы ETL-конвейеров (DAG).  
Спроектировать архитектуру аналитического решения.  
Реализовать ETL-процесс получения погодных данных через API и их обработки.  
Использовать обученную ML-модель для прогнозирования бизнес-метрик (продаж).  

*Задание*:  
Прогноз: Афины, 3 дня	Округлить t	Линейный график по дням  

## Архитектура решения

```mermaid
graph LR
    subgraph Source[Source Layer]
        A[Open-Meteo API<br/>Источник данных]
    end

    subgraph Storage[Storage Layer]
        B[(CSV Files<br/>Сырые и очищенные данные)]
        C[(ml_model.pkl<br/>Модель)]
    end

    subgraph Business[Business Layer]
        D[Streamlit<br/>Дашборд]
        E[Jupyter Notebook<br/>Прогноз продаж]
    end

    A -->|ETL| B
    B -->|обучение| C
    C -->|предсказание| E
    B -->|визуализация| D
```

## 1.2. Dockerfile
```bash
FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    psycopg2-binary \
    streamlit \
    matplotlib \
    plotly \
    "connexion[swagger-ui]"

USER root

RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow
```

## 1.3. docker-compose.yml (ключевые сервисы)
postgres: база данных для метаданных Airflow  
init: инициализация базы данных и создание пользователя   
webserver: веб-интерфейс Airflow (порт 8080)  
scheduler: планировщик задач  
streamlit: дашборд визуализации (порт 8501)  

```bash
x-environment: &airflow_environment
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
  - AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey123

x-airflow-image: &airflow_image custom-airflow:slim-2.8.1-python3.11

services:
  postgres:
    image: postgres:12-alpine
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org"

  webserver:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: webserver

  scheduler:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: scheduler

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"
```
<img width="571" height="445" alt="image" src="https://github.com/user-attachments/assets/de6a90bd-cb0f-41a0-b45b-4186ff63c5cc" />

## dags/variant_09.py
<details>
  <summary> <u> Код DAG </u> </summary>
  
  ```r
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
import joblib
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.linear_model import LinearRegression

CITY = "Athens"
DAYS = 14

DATA_DIR = '/opt/airflow/data'
WEATHER_FILE = os.path.join(DATA_DIR, 'weather_forecast.csv')
CLEAN_WEATHER_FILE = os.path.join(DATA_DIR, 'clean_weather.csv')
SALES_FILE = os.path.join(DATA_DIR, 'sales_data.csv')
CLEAN_SALES_FILE = os.path.join(DATA_DIR, 'clean_sales.csv')
JOINED_FILE = os.path.join(DATA_DIR, 'joined_data.csv')
MODEL_FILE = os.path.join(DATA_DIR, 'ml_model.pkl')

os.makedirs(DATA_DIR, exist_ok=True)


def fetch_weather_forecast():
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DAYS)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 37.9838,
        "longitude": 23.7275,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": "temperature_2m_mean",
        "timezone": "Europe/Moscow"
    }

    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame({
        'date': data['daily']['time'],
        'temperature': data['daily']['temperature_2m_mean']
    })

    df.to_csv(WEATHER_FILE, index=False)
    print(f"Загружено {len(df)} дней данных для Афин")


def clean_weather_data():
    df = pd.read_csv(WEATHER_FILE)

    df['temp_original'] = df['temperature']
    df['temp_rounded'] = df['temperature'].round(0).astype(int)

    df['date'] = pd.to_datetime(df['date'])
    days_map = {
        0: 'Понедельник', 1: 'Вторник', 2: 'Среда',
        3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'
    }
    df['день недели'] = df['date'].dt.weekday.map(days_map)

    df.to_csv(CLEAN_WEATHER_FILE, index=False)
    print(f"Данные обработаны, сохранены в {CLEAN_WEATHER_FILE}")


def fetch_sales_data():
    weather_df = pd.read_csv(WEATHER_FILE)

    import numpy as np
    np.random.seed(42)

    sales = 100 + 5 * weather_df['temperature'] + np.random.randn(len(weather_df)) * 5

    df = pd.DataFrame({
        'date': weather_df['date'],
        'sales': sales
    })

    df.to_csv(SALES_FILE, index=False)
    print(f"Сгенерировано {len(df)} записей о продажах")


def clean_sales_data():
    df = pd.read_csv(SALES_FILE)
    df.to_csv(CLEAN_SALES_FILE, index=False)
    print(f"Данные о продажах сохранены в {CLEAN_SALES_FILE}")


def join_datasets():
    weather_df = pd.read_csv(CLEAN_WEATHER_FILE)
    sales_df = pd.read_csv(CLEAN_SALES_FILE)

    joined_df = pd.merge(weather_df, sales_df, on='date')
    joined_df.to_csv(JOINED_FILE, index=False)
    print(f"Объединено {len(joined_df)} записей")


def train_ml_model():
    df = pd.read_csv(JOINED_FILE)

    X = df[['temp_rounded']]
    y = df['sales']

    model = LinearRegression()
    model.fit(X, y)

    joblib.dump(model, MODEL_FILE)

    print(f"Модель обучена на {len(df)} наблюдениях")
    print(f"Коэффициент: {model.coef_[0]:.2f}")
    print(f"Интерсепт: {model.intercept_:.2f}")


def deploy_ml_model():
    model = joblib.load(MODEL_FILE)
    print(f"Модель загружена. Коэффициент: {model.coef_[0]:.2f}, Интерсепт: {model.intercept_:.2f}")


default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'variant_09_athens_weather',
    default_args=default_args,
    description='Прогноз погоды Афины, 14 дней',
    schedule_interval=None,
    catchup=False,
)

t1 = PythonOperator(task_id="fetch_weather_forecast", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="clean_weather_data", python_callable=clean_weather_data, dag=dag)
t3 = PythonOperator(task_id="fetch_sales_data", python_callable=fetch_sales_data, dag=dag)
t4 = PythonOperator(task_id="clean_sales_data", python_callable=clean_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_datasets", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model, dag=dag)
t7 = PythonOperator(task_id="deploy_ml_model", python_callable=deploy_ml_model, dag=dag)

t1 >> t2
t3 >> t4
[t2, t4] >> t5
t5 >> t6 >> t7
  ```
  
</details>

## app/app.py (Streamlit дашборд)
<details>
  <summary> <u> Код Streamlit </u> </summary>
  
  ```r
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Прогноз погоды Афины", layout="wide")
st.title("Анализ погоды в Афинах (Вариант 9)")

data_path = '/opt/airflow/data/clean_weather.csv'

if os.path.exists(data_path):
    df = pd.read_csv(data_path)

    st.write("### Очищенные данные с округленной температурой")
    st.dataframe(df)

    st.write("### График: Температура по дням")

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.plot(df['date'], df['temp_original'],
            marker='s', linewidth=1.5, color='blue', alpha=0.6,
            linestyle='--', label='Исходная')

    ax.plot(df['date'], df['temp_rounded'],
            marker='o', linewidth=2, color='orange', label='Округленная')

    ax.set_xlabel('Дата')
    ax.set_ylabel('Температура (°C)')
    ax.set_title('Прогноз температуры в Афинах')
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    st.pyplot(fig)

    st.write("### Статистика")
    st.write(f"Средняя температура: {df['temp_rounded'].mean():.1f}°C")
    st.write(f"Максимальная температура: {df['temp_rounded'].max()}°C")
    st.write(f"Минимальная температура: {df['temp_rounded'].min()}°C")

else:
    st.warning("Данные еще не сгенерированы. Запустите DAG в Airflow.")
  ```
  
</details>

## 1.4. Запуск
```bash
docker build -t custom-airflow:slim-2.8.1-python3.11 .
docker compose up -d
```

<img width="586" height="354" alt="image" src="https://github.com/user-attachments/assets/197b3870-c912-4f93-948c-90e0d01c3f91" />  
<img width="585" height="191" alt="image" src="https://github.com/user-attachments/assets/e6669be7-bf76-46d4-aa07-e5ba4e715188" />  

*Результат: Airflow доступен по адресу http://localhost:8080, логин admin, пароль admin.*

## Структура DAG
<img width="1011" height="437" alt="image" src="https://github.com/user-attachments/assets/80756dea-4de8-4a9e-9506-3ca584574c23" />  

*DAG успешно запущен*  
<img width="1459" height="772" alt="image" src="https://github.com/user-attachments/assets/25085a99-357c-4ee6-abf2-9cfb003c60c8" />  

## Streamlit (Дашборд)  
<img width="1439" height="684" alt="image" src="https://github.com/user-attachments/assets/f33c5dad-493f-498e-aa87-4bd355a34a4a" />  
<img width="1407" height="524" alt="image" src="https://github.com/user-attachments/assets/a6726775-5522-435e-9e68-b52a3509e64a" />  
<img width="1422" height="406" alt="image" src="https://github.com/user-attachments/assets/2450158c-6936-4ba9-80cb-758993ff15da" />



## ML Аналитика
После успешного выполнения DAG variant_09_athens_weather в папке data была создана обученная модель ml_model.pkl. Модель представляет собой линейную регрессию, обученную на 14 наблюдениях (температура и соответствующие продажи).  
*Параметры модели:*  
Коэффициент: 4.94  
Интерсепт: 101.74  
Формула: Продажи = 4.94 * temp_rounded + 101.74  
*Интерпретация:*  
При увеличении температуры на 1 градус продажи растут на 4.94 единицы  
Базовый уровень продаж при 0 градусах составляет 101.74  

### Загрузка модели в [Google Collab](https://colab.research.google.com/drive/1J6zJRvRiZcX2UYFegS2UmWO9zZMLSy-n?usp=sharing)  
```bash
print(" Загрузите файл ml_model.pkl")
uploaded = files.upload()

file_name = list(uploaded.keys())[0]
model = joblib.load(file_name)

print(f" Модель загружена: {file_name}")
print(f"   Коэффициент: {model.coef_[0]:.2f}")
print(f"   Интерсепт: {model.intercept_:.2f}")
```

Результат:
<img width="359" height="76" alt="image" src="https://github.com/user-attachments/assets/654bced0-6a6c-47c0-aef4-04bf9b95706e" />  

### Прогноз по заданию (температура 15°C)
В соответствии с индивидуальным заданием (вариант 9) выполнен прогноз продаж при температуре 15°C.  
```bash
my_temp = 15
input_data = pd.DataFrame({'temp_rounded': [my_temp]})
prediction = model.predict(input_data)[0]

print(f"При температуре {my_temp}°C прогнозируемые продажи составят: {prediction:.2f}")
```
<img width="516" height="73" alt="image" src="https://github.com/user-attachments/assets/75c9bdc6-abb3-4d7a-8405-61b5ab89b6cf" />  

### Прогноз по реальным данным (средняя температура 13°C)
На основе реальных данных из DAG (средняя температура за 14 дней) выполнен прогноз.

```bash
avg_temp = 13
input_data_avg = pd.DataFrame({'temp_rounded': [avg_temp]})
avg_prediction = model.predict(input_data_avg)[0]

print(f"Средняя температура в Афинах: {avg_temp}°C")
print(f"Прогнозируемые продажи: {avg_prediction:.2f}")
```
<img width="481" height="108" alt="image" src="https://github.com/user-attachments/assets/57fa05e6-2b90-4762-b5b5-b5df2f128829" />  

### Визуализация
<img width="868" height="501" alt="image" src="https://github.com/user-attachments/assets/56430a19-a22e-434f-8702-bd37a6a7c709" />  




