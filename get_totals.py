#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3

from core.config import fact_date1
from core.config import fact_date2
from core.config import forecast_date1
from core.config import forecast_date2
from database.db import get_connection

# Подключаемся к БД
try:
    con, cursor = get_connection()

    # Агрегатная функция суммы
    req_fact_qliq_date1 = f"SELECT SUM(data1) FROM data WHERE model = 'fact' AND product = 'Qliq' AND date1 = '{fact_date1}'"  # Формируем запрос
    fact_qliq_date1 = cursor.execute(req_fact_qliq_date1).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FACT Qliq на дату {fact_date1}: {fact_qliq_date1[0]}"
    )  # Выводим результат

    req_fact_qliq_date2 = f"SELECT SUM(data2) FROM data WHERE model = 'fact' AND product = 'Qliq' AND date2 = '{fact_date2}'"  # Формируем запрос
    fact_qliq_date2 = cursor.execute(req_fact_qliq_date2).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FACT Qliq на дату {fact_date2}: {fact_qliq_date2[0]}"
    )  # Выводим результат

    req_fact_Qoil_date1 = f"SELECT SUM(data1) FROM data WHERE model = 'fact' AND product = 'Qoil' AND date1 = '{fact_date1}'"  # Формируем запрос
    fact_qoil_date1 = cursor.execute(req_fact_Qoil_date1).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FACT Qoil на дату {fact_date1}: {fact_qoil_date1[0]}"
    )  # Выводим результатат

    req_fact_Qoil_date2 = f"SELECT SUM(data2) FROM data WHERE model = 'fact' AND product = 'Qoil' AND date2 = '{fact_date2}'"  # Формируем запрос
    fact_qoil_date2 = cursor.execute(req_fact_Qoil_date2).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FACT Qoil на дату {fact_date2}: {fact_qoil_date2[0]}"
    )  # Выводим результатат

    req_forecast_qliq_date1 = f"SELECT SUM(data1) FROM data WHERE model = 'forecast' AND product = 'Qliq' AND date1 = '{forecast_date1}'"  # Формируем запрос
    forecast_qliq_date1 = cursor.execute(
        req_forecast_qliq_date1
    ).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FORECAST Qliq на дату {forecast_date1}: {forecast_qliq_date1[0]}"
    )  # Выводим результат

    req_forecast_qliq_date2 = f"SELECT SUM(data2) FROM data WHERE model = 'forecast' AND product = 'Qliq' AND date2 = '{forecast_date2}'"  # Формируем запрос
    forecast_qliq_date2 = cursor.execute(
        req_forecast_qliq_date2
    ).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FORECAST Qliq на дату {forecast_date2}: {forecast_qliq_date2[0]}"
    )  # Выводим результат

    req_forecast_qoil_date1 = f"SELECT SUM(data1) FROM data WHERE model = 'forecast' AND product = 'Qoil' AND date1 = '{forecast_date1}'"  # Формируем запрос
    forecast_qoil_date1 = cursor.execute(
        req_forecast_qoil_date1
    ).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FORECAST Qoil на дату {forecast_date1}: {forecast_qoil_date1[0]}"
    )  # Выводим результат

    req_forecast_qoil_date2 = f"SELECT SUM(data2) FROM data WHERE model = 'forecast' AND product = 'Qoil' AND date2 = '{forecast_date2}'"  # Формируем запрос
    forecast_qoil_date2 = cursor.execute(
        req_forecast_qoil_date2
    ).fetchone()  # Выполняем запрос
    print(
        f"Общая сумма FORECAST Qoil на дату {forecast_date2}: {forecast_qoil_date2[0]}"
    )  # Выводим результат

    cursor.close()
except sqlite3.Error as error:
    print("Ошибка при подключении к sqlite", error)
finally:
    if con:
        con.close()
