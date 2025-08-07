from flask import Flask, request, redirect, url_for
from datetime import datetime

from sheets import (
    load_transactions_from_sheet,
    load_regular_income,
    load_irregular_income,
    load_regular_payments,
    load_loans,
)
from db import (recreate_db, save_dataframe, read_transactions,
                check_table_exists, calculate_current_balance, read_table)

app = Flask(__name__)


# 📍 Главная страница
@app.route("/", methods=["GET"])
def home():
    message = request.args.get("message")

    current_month = datetime.today().strftime('%Y-%m')

    # Если таблиц нет — пересоздаём и загружаем
    if not check_table_exists():
        recreate_db()
        update_all_data()

    df = read_transactions(month=current_month)

    balance = calculate_current_balance()

    html = f"""
    <h2>Траты за текущий месяц</h2>
    <p><b>💰 Баланс на сегодня:</b> {balance:,.2f} ₽</p>
    

    <form action="/update-db" method="post">
        <button type="submit">🔄 Обновить из Google Sheets</button>
    </form>
    <br>
    """

    if message:
        html += f"<p style='color: green; font-weight: bold;'>✅ {message}</p>"

    html += df.to_html(index=False)

    # Вывод тест таблицы
    df_salary = read_table("regular_payments").sort_values(
        "Дата", ascending=True).tail(5)

    html += "<h3>Просмотр таблиц</h3>"
    html += df_salary.to_html(index=False)
    
    return html


# 📍 Обновление базы
@app.route("/update-db", methods=["POST"])
def update_db():
    recreate_db()
    update_all_data()
    return redirect(url_for("home", message="Данные обновлены!"))


# 📍 Функция загрузки всех таблиц
def update_all_data():
    save_dataframe(load_transactions_from_sheet(), "transactions")
    save_dataframe(load_regular_income(), "regular_income")
    save_dataframe(load_irregular_income(), "irregular_income")
    save_dataframe(load_regular_payments(), "regular_payments")
    save_dataframe(load_loans(), "loans")


# 📍 Запуск
app.run(host="0.0.0.0", port=81)

