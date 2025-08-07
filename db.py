import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_FILE = "finances.db"
INITIAL_BALANCE = 25217.0

RENAME_MAP = {
    "Дата": "Дата",
    "День недели": "День_недели",
    "Категории": "Категория",
    "Группа категории": "Группа",
    "Описание платежа": "Описание",
    "Источник": "Источник",
    "Комментарий": "Комментарий",
    "Плановая дата": "Плановая_дата",
    "Плановый день недели": "Плановый_день_недели",
    "Фактическая дата": "Фактическая_дата",
    "Сумма": "Сумма",
    "Ставка": "Ставка",
    "Первоначальная сумма": "Первоначальная_сумма",
    "Дата открытия": "Дата_открытия",
    "Дата окончания": "Дата_окончания",
    "Срок кредита, мес.": "Срок_мес",
    "Дней до платежа": "Дней_до_платежа",
    "Осталось платежей": "Осталось_платежей",
    "Номер платежа": "Номер_платежа",
    "% выплаты": "Процент_выплаты"
}

ALLOWED_COLUMNS = {
    "transactions":
    {"Дата", "День_недели", "Категория", "Группа", "Описание", "Сумма"},
    "regular_income": {"Дата", "День_недели", "Источник", "Сумма"},
    "irregular_income": {"Дата", "Источник", "Сумма"},
    "regular_payments":
    {"Дата", "День_недели", "Описание", "Сумма", "Комментарий"},
    "loans": {
        "Плановая_дата", "Плановый_день_недели", "Фактическая_дата",
        "Описание", "Сумма", "Ставка", "Первоначальная_сумма", "Дата_открытия",
        "Дата_окончания", "Срок_мес", "Дней_до_платежа", "Осталось_платежей",
        "Номер_платежа", "Процент_выплаты"
    }
}


def recreate_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE transactions (
            Дата DATE,
            День_недели TEXT,
            Категория TEXT,
            Группа TEXT,
            Описание TEXT,
            Сумма REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE regular_income (
            Дата DATE,
            День_недели TEXT,
            Источник TEXT,
            Сумма REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE irregular_income (
            Дата DATE,
            Источник TEXT,
            Сумма REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE regular_payments (
            Дата DATE,
            День_недели TEXT,
            Описание TEXT,
            Сумма REAL,
            Комментарий TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE loans (
            Плановая_дата DATE,
            Плановый_день_недели TEXT,
            Фактическая_дата DATE,
            Описание TEXT,
            Сумма REAL,
            Ставка REAL,
            Первоначальная_сумма REAL,
            Дата_открытия DATE,
            Дата_окончания DATE,
            Срок_мес INTEGER,
            Дней_до_платежа INTEGER,
            Осталось_платежей INTEGER,
            Номер_платежа INTEGER,
            Процент_выплаты REAL
        )
    """)

    conn.commit()
    conn.close()


def save_dataframe(df: pd.DataFrame, table_name: str):
    if table_name not in ALLOWED_COLUMNS:
        raise ValueError(f"Неизвестная таблица: {table_name}")

    # Переименование
    df = df.rename(columns={
        k: v
        for k, v in RENAME_MAP.items() if k in df.columns
    })

    # Фильтрация по нужным колонкам
    df = df[[col for col in df.columns if col in ALLOWED_COLUMNS[table_name]]]

    # Очистка "Сумма"
    if "Сумма" in df.columns:
        df["Сумма"] = (df["Сумма"].astype(str).str.replace(
            "р.", "",
            regex=False).str.replace(" ", "",
                                     regex=False).str.replace("\xa0",
                                                              "",
                                                              regex=False))
        df = df[df["Сумма"].str.strip() != ""]
        df["Сумма"] = pd.to_numeric(df["Сумма"], errors="coerce")
        df = df.dropna(subset=["Сумма"])

    # Преобразование дат
    for col in df.columns:
        if "дата" in col.lower():
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            df = df.dropna(subset=[col])
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    conn = sqlite3.connect(DB_FILE)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def read_transactions(month: str = None) -> pd.DataFrame:
    conn = sqlite3.connect(DB_FILE)
    if month:
        query = """
            SELECT * FROM transactions
            WHERE strftime('%Y-%m', "Дата") = ?
            ORDER BY "Дата" DESC
        """
        df = pd.read_sql(query, conn, params=(month, ))
    else:
        df = pd.read_sql("SELECT * FROM transactions ORDER BY \"Дата\" DESC",
                         conn)
    conn.close()
    return df


def read_table(table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def check_table_exists(table_name="transactions") -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?;
    """, (table_name, ))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def calculate_current_balance() -> float:
    import calendar
    from datetime import datetime
    import sqlite3

    today = datetime.today().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_FILE)

    def sum_query(table: str,
                  date_col: str = "Дата",
                  start_date: str = None,
                  end_date: str = None,
                  column: str = "Сумма") -> float:
        query = f"SELECT SUM({column}) FROM {table} WHERE 1=1"
        params = []

        if start_date:
            query += f" AND date({date_col}) >= date(?)"
            params.append(start_date)
        if end_date:
            query += f" AND date({date_col}) <= date(?)"
            params.append(end_date)

        result = conn.execute(query, params).fetchone()[0]
        return float(result) if result else 0.0

    # Автоматически определяем начало и конец месяца
    month = "2025-08"
    year, mon = map(int, month.split("-"))
    start = f"{month}-01"
    end = f"{month}-{calendar.monthrange(year, mon)[1]:02d}"  # Авто: 31, 30, 28 и т.д.

    start = "2025-01-01"
    end = today

    total = (
        sum_query("regular_income", date_col="Дата", start_date=start, end_date=end) +
        sum_query("irregular_income", date_col="Дата", start_date=start, end_date=end) -
        sum_query("transactions", date_col="Дата", start_date=start, end_date=end) -
        sum_query("regular_payments", date_col="Дата", start_date=start, end_date=end) -
        sum_query("loans", date_col="Плановая_дата", start_date=start, end_date=end)
    )

    conn.close()
    return round(INITIAL_BALANCE + total, 2)