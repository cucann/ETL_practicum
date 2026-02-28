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


**Доп. Задание 1 — Отчет по менеджерам**
Результат:  
<img width="1187" height="362" alt="image" src="https://github.com/user-attachments/assets/193fa5c7-00d1-4c7c-b929-fcd636e49d6c" />  
<img width="1192" height="347" alt="image" src="https://github.com/user-attachments/assets/b9115dab-44c0-40ec-8028-882677ca40d1" />  


**Вывод:** по результатам анализа лидером по прибыли является Chuck Magee (564 051 у.е.) с наибольшей маржинальностью - 27,1%, тогда как максимальный объём продаж (около 4,2 млн у.е.) и количество заказов (≈19 000) демонстрирует Anna Andreadi при прибыли 524 015 у.е. Kelly Williams (376 496 у.е.) и Cassandra Brandow (281 229 у.е.) показывают более умеренные результаты; при этом средний чек колеблется в диапазоне 218–233 у.е., а средний уровень скидок у всех менеджеров находится примерно на уровне 16–18%, что говорит о сопоставимой ценовой политике.  


**Доп. задание 2 — Анализ категорий**  
Результат:  
<img width="804" height="317" alt="image" src="https://github.com/user-attachments/assets/1444f3da-454d-44cb-8dfd-01df0b5dd065" />  
<img width="1180" height="320" alt="image" src="https://github.com/user-attachments/assets/6d444a09-c24f-471a-b012-e042eeb066cd" />  
<img width="1180" height="280" alt="image" src="https://github.com/user-attachments/assets/eb29f3c7-3de3-41b5-82da-bd2634cc8eae" />  

**Вывод:** наибольшую долю в структуре продаж занимает категория Furniture (35,3%), при объёме продаж 4 832,1 тыс. у.е., однако максимальную прибыль приносит Office Supplies — 802,4 тыс. у.е. при продажах 4 780,3 тыс. у.е. Категория Technology имеет наименьшую долю продаж (29,7%) и объём 4 065,3 тыс. у.е., при этом её прибыль (789,2 тыс. у.е.) сопоставима с Office Supplies, что говорит о более высокой рентабельности по сравнению с Furniture (154,2 тыс. у.е.).  

**Заключение**  
Компании рекомендуется масштабировать практики наиболее маржинального менеджера Chuck Magee при сохранении объёмной стратегии лидера по продажам и сосредоточить инвестиции на более прибыльных категориях (Office Supplies и Technology), одновременно повышая маржинальность направления Furniture.


## Приложения к лабораторной работе:  

