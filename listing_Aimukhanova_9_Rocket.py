from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from pathlib import Path
import hashlib
import os


DATA_DIR = Path("/opt/airflow/data")
ALL_IMAGES_DIR = DATA_DIR / "all_images"
FAILED_LOG_PATH = DATA_DIR / "failed_images.csv"
LAUNCHES_JSON = DATA_DIR / "launches.json"
API_URL = "https://ll.thespacedevs.com/2.2.0/launch/upcoming"

default_args = {
    'owner': 'Aimukhanova',
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
}

# Агрегация изображений в одну папку
def aggregate_images(**context):
    """Скачивает в папку data/all_images/"""
    
   
    ALL_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Папка для агрегации: {ALL_IMAGES_DIR}")
    
    print("Загружаем данные из API...")
    response = requests.get(API_URL, timeout=30)
    data = response.json()
    
    with open(LAUNCHES_JSON, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Сохранен {LAUNCHES_JSON}")
    
    image_urls = []
    for launch in data.get('results', []):
        if launch.get('image'):
            image_urls.append(launch['image'])
        if launch.get('patch', {}).get('image'):
            image_urls.append(launch['patch']['image'])
        if launch.get('rocket', {}).get('configuration', {}).get('image_url'):
            image_urls.append(launch['rocket']['configuration']['image_url'])
    
    image_urls = list(set(image_urls))
    print(f"Найдено уникальных изображений: {len(image_urls)}")
    
    successful = []
    failed = []
    
    for idx, url in enumerate(image_urls):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"rocket_{idx:03d}_{url_hash}.jpg"
        filepath = ALL_IMAGES_DIR / filename
        
        try:
            img_response = requests.get(url, timeout=15, stream=True)
            img_response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in img_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            if filepath.stat().st_size > 100:
                successful.append(url)
                print(f"OK [{idx+1}/{len(image_urls)}] {filename}")
            else:
                filepath.unlink()
                raise Exception("Файл слишком маленький")
                
        except Exception as e:
            failed.append({'url': url, 'error': str(e)})
            print(f"FAIL [{idx+1}/{len(image_urls)}] Ошибка: {str(e)[:50]}")
    
    context['ti'].xcom_push(key='success_count', value=len(successful))
    context['ti'].xcom_push(key='fail_count', value=len(failed))
    context['ti'].xcom_push(key='failed_list', value=failed)
    
    print(f"ИТОГО: Успешно {len(successful)}, Ошибок {len(failed)}")
    return f"Скачано {len(successful)} изображений в {ALL_IMAGES_DIR}"

# Лог недоступных изображений
def log_failed(**context):
    """Сохраняет список недоступных изображений в CSV"""
    
    failed_list = context['ti'].xcom_pull(key='failed_list', task_ids='aggregate_images')
    
    if failed_list:
        df = pd.DataFrame(failed_list)
        df.to_csv(FAILED_LOG_PATH, index=False)
        print(f"Сохранено {len(failed_list)} ошибок в {FAILED_LOG_PATH}")
        print("Первые 3 ошибки:")
        for i, fail in enumerate(failed_list[:3]):
            print(f"  {i+1}. {fail['url'][:80]}... -> {fail['error']}")
    else:
        pd.DataFrame(columns=['url', 'error']).to_csv(FAILED_LOG_PATH, index=False)
        print("Нет недоступных изображений")

# Уведомление о завершении всех процессов
def send_notification(**context):
    """Отправляет уведомление со статистикой"""
    
    success = context['ti'].xcom_pull(key='success_count', task_ids='aggregate_images') or 0
    fail = context['ti'].xcom_pull(key='fail_count', task_ids='aggregate_images') or 0
    
    if ALL_IMAGES_DIR.exists():
        files_count = len(list(ALL_IMAGES_DIR.glob("*")))
    else:
        files_count = 0
    
    notification = f"""
Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

1 - (Агрегация):
   Папка с изображениями: {ALL_IMAGES_DIR}
   Успешно скачано: {success}
   Всего файлов в папке: {files_count}

2 - (Лог ошибок):
   Файл с ошибками: {FAILED_LOG_PATH}
   Количество ошибок: {fail}

3 - (Уведомление):
   Все процессы завершены

"""
    
    print(notification)
    
    with open(DATA_DIR / "notification.txt", 'w') as f:
        f.write(notification)

# Создаем DAG
with DAG(
    dag_id='listing_Aimukhanova_9_Rocket',
    default_args=default_args,
    description='Вариант 9: Агрегация + Лог ошибок + Уведомление',
    schedule_interval='@daily',
    catchup=False,
    tags=['variant_9', 'rocket'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    task_aggregate = PythonOperator(
        task_id='aggregate_images',
        python_callable=aggregate_images,
    )
    
    task_log = PythonOperator(
        task_id='log_failed_images',
        python_callable=log_failed,
    )
    
    task_notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> task_aggregate >> task_log >> task_notify >> end
