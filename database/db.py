#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
from core.config import BASE_DIR
from pathlib import Path


db_path = Path(BASE_DIR, "database", "ursip_db.db")


def get_connection():
    con = sqlite3.connect(db_path)
    cursor = con.cursor()
    return con, cursor
