#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path

import pandas as pd

from core.config import BASE_DIR
from core.config import fact_date1
from core.config import fact_date2
from core.config import forecast_date1
from core.config import forecast_date2
from database.db import get_connection


# Читаем эксель файл
excel_path = Path(BASE_DIR, "excel_data", "data.xlsx")
f = pd.read_excel(excel_path)


# Разбор данных из файла
columns = f.columns.values.tolist()  # Список столбцов


# Данные столбцов
company = f.get(columns[1])
fact_qliq_data1 = f.get(columns[2])
fact_qliq_data2 = f.get(columns[3])
fact_qoil_data1 = f.get(columns[4])
fact_qoil_data2 = f.get(columns[5])
forecast_qliq_data1 = f.get(columns[6])
forecast_qliq_data2 = f.get(columns[7])
forecast_qoil_data1 = f.get(columns[8])
forecast_qoil_data2 = f.get(columns[9])


# Подготовим данные для таблицы БД
num = len(company) - 2
temp_data = []


for i in range(num):
    temp_tuple = (
        company[i + 2],
        columns[2],
        "Qliq",
        fact_date1,
        fact_qliq_data1[i + 2],
        fact_date2,
        fact_qliq_data2[i + 2],
    )
    temp_data.append(temp_tuple)


for i in range(num):
    temp_tuple = (
        company[i + 2],
        columns[2],
        "Qoil",
        fact_date1,
        fact_qoil_data1[i + 2],
        fact_date2,
        fact_qoil_data2[i + 2],
    )
    temp_data.append(temp_tuple)


for i in range(num):
    temp_tuple = (
        company[i + 2],
        columns[6],
        "Qliq",
        forecast_date1,
        forecast_qliq_data1[i + 2],
        forecast_date2,
        forecast_qliq_data2[i + 2],
    )
    temp_data.append(temp_tuple)


for i in range(num):
    temp_tuple = (
        company[i + 2],
        columns[6],
        "Qoil",
        forecast_date1,
        forecast_qoil_data1[i + 2],
        forecast_date2,
        forecast_qoil_data2[i + 2],
    )
    temp_data.append(temp_tuple)


# Подключаемся к БД
try:
    con, cursor = get_connection()
    print("Соединение с SQLite установлено")

    # Создаем таблицу
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS data
                    (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT,
                    model TEXT,
                    product TEXT,
                    date1 TEXT,
                    data1 INTEGER,
                    date2 TEXT,
                    data2 INTEGER
                    )
                """
    )

    # Вносим данные в таблицу
    cursor.executemany(
        "INSERT INTO data (company, model, product, date1, data1, date2, data2) VALUES (?, ?, ?, ?, ?, ?, ?)",
        temp_data,
    )
    con.commit()
    print(
        "Данные в таблицу внесены успешно.\nДля рассчета тоталов, запустите get_totals.py"
    )
    cursor.close()

except sqlite3.Error as error:
    print("Ошибка при подключении к sqlite", error)
finally:
    if con:
        con.close()
        print("Соединение с SQLite закрыто")
