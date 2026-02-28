# Лабораторная работа №2
## Динамические соединения с базами данных в Pentaho Data Integration

**Вариант 9**: фильтр по returned = 0 (только заказы без возвратов)

---

## 1. Цель и задачи работы

**Цель:** получение практических навыков создания ETL-процесса для загрузки данных из CSV-файла в базу данных MySQL с использованием Pentaho Data Integration:

**Только заказы без возвратов**  
**Отчет по менеджерам (Аналитика)**  
**Анализ категорий (Аналитика)**  

---

## 2. Архитектура решения
### Реализация основного фильтра для загрузки в БД

Трансформация lab_02_1_csv_orders 
<img width="960" height="465" alt="image" src="https://github.com/user-attachments/assets/acab8fa3-76e8-4884-bad2-b84e0e7948a7" />  

Выбранные поля из csv  
<img width="1134" height="323" alt="image" src="https://github.com/user-attachments/assets/6892ad7c-44b1-4ca8-9ce9-aa98f031cce7" />

Value Mapper (чистка NULL в таблице)  
<img width="837" height="392" alt="image" src="https://github.com/user-attachments/assets/afcef2e9-ce63-4ed2-8e87-b33ada99e899" />  

Filter Rows  
<img width="737" height="338" alt="image" src="https://github.com/user-attachments/assets/5fd40526-f966-4dd8-a6a7-37059e02c419" />  
Условие: returned = 0 (только заказы без возвратов)

В трансформациях customers и products добавлен Unique Rows (дедупликация)  
<img width="927" height="412" alt="image" src="https://github.com/user-attachments/assets/bedbdcac-65ef-40cb-bcef-ff39c9eaed35" />  

<img width="495" height="329" alt="image" src="https://github.com/user-attachments/assets/fbb24369-47af-479e-9c1d-54059529aded" />

Выведен Write to Log для ошибок
<img width="940" height="345" alt="image" src="https://github.com/user-attachments/assets/2ef15096-fa0b-471f-94ab-9e48962eae00" />


**Запуск Job**  
<img width="1059" height="554" alt="image" src="https://github.com/user-attachments/assets/82ba4167-0b8d-4e6d-b723-c9cd214708e9" />
<img width="1054" height="295" alt="image" src="https://github.com/user-attachments/assets/0c6e8fdf-1a87-45ed-aa40-994ff5466566" />

модуль HTTP:  
<img width="1220" height="694" alt="image" src="https://github.com/user-attachments/assets/cba4f40a-cd5c-4fdc-a279-a9ecad9d571c" />





