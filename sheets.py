import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# Названия листов с августа 2025 по март 2026
MONTHLY_SHEETS = [
    "НТ (Август 2025)", "НТ (Сентябрь 2025)", "НТ (Октябрь 2025)",
    "НТ (Ноябрь 2025)", "НТ (Декабрь 2025)", "НТ (Январь 2026)",
    "НТ (Февраль 2026)", "НТ (Март 2026)"
]

STANDARD_COLUMNS = [
    "Дата", "День недели", "Категории", "Группа категории", "Описание платежа",
    "Сумма"
]

# Авторизация и подключение к Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    "feisty-proton-467913-e4-12302e1de474.json", scope)
client = gspread.authorize(creds)
print("Авторизация прошла. Пробуем открыть таблицу...")
spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/17iDiNgyxo_GpAUFYvQ15zTru4dLpyOJrX810Wuyb_6s/edit"
)


# Универсальная функция очистки сумм и дат
def clean_amount_and_date(df: pd.DataFrame,
                          date_col="Дата",
                          amount_col="Сумма") -> pd.DataFrame:
    df = df[df[date_col].str.strip() != ""]
    df = df[df[amount_col].astype(str).str.strip() != ""]
    df[amount_col] = (df[amount_col].astype(str).str.replace(
        "р.", "", regex=False).str.replace(
            "\xa0", "", regex=False).str.replace(" ", "",
                                                 regex=False).str.strip())
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
    df = df.dropna(subset=[amount_col])
    df[date_col] = pd.to_datetime(df[date_col], format="%d.%m.%Y", errors="coerce")
    return df


# Загрузка расходов из всех листов
def load_transactions_from_sheet():
    all_dataframes = []
    try:
        old_sheet = spreadsheet.worksheet("Нерегулярные траты")
        old_rows = old_sheet.get_all_values()
        trimmed = [row[:6] for row in old_rows]
        df_old = pd.DataFrame(trimmed[1:], columns=STANDARD_COLUMNS)
        df_old = clean_amount_and_date(df_old)
        all_dataframes.append(df_old)
    except gspread.exceptions.WorksheetNotFound:
        print("Лист 'Нерегулярные траты' не найден")

    for name in MONTHLY_SHEETS:
        try:
            ws = spreadsheet.worksheet(name)
            rows = ws.get_all_values()
            trimmed = [row[:6] for row in rows]
            df_month = pd.DataFrame(trimmed[1:], columns=STANDARD_COLUMNS)
            df_month = clean_amount_and_date(df_month)
            all_dataframes.append(df_month)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Лист '{name}' не найден")

    return (pd.concat(all_dataframes, ignore_index=True)
            if all_dataframes else pd.DataFrame(columns=STANDARD_COLUMNS))


def load_regular_income():
    ws = spreadsheet.worksheet("Регулярный приход")
    rows = ws.get_all_values()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = clean_amount_and_date(df)
    return df


def load_irregular_income():
    ws = spreadsheet.worksheet("Нерегулярный приход")
    rows = ws.get_all_values()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = clean_amount_and_date(df)
    return df


def load_regular_payments():
    ws = spreadsheet.worksheet("Регулярные платежи")
    rows = ws.get_all_values()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = clean_amount_and_date(df)
    return df


def load_loans():
    ws = spreadsheet.worksheet("Кредиты")
    rows = ws.get_all_values()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = clean_amount_and_date(df,
                               date_col="Плановая дата",
                               amount_col="Сумма")
    return df
