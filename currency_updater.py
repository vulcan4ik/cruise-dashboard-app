# currency_updater.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from xml.etree import ElementTree as ET
import os

# Пути к данным и логам
RATES_FILE = '/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'
LOG_FILE = '/home/vulcan4ik/dashboard-cruise-app/app_data/currency_updater.log'


def log_message(message):
    """Запись сообщений в лог-файл"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}\n")
    print(message)


def get_cbr_rates_for_date(date):
    """Получение курсов ЦБ за один день (USD и EUR одним запросом)"""
    url = "http://www.cbr.ru/scripts/XML_daily.asp"
    params = {'date_req': date.strftime('%d/%m/%Y')}
    rates = {}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        for valute in root.findall('Valute'):
            code = valute.find('CharCode').text
            if code in ('USD', 'EUR'):
                value = float(valute.find('Value').text.replace(',', '.'))
                rates[code] = value

    except Exception as e:
        log_message(f"❌ Ошибка получения курсов на {date.strftime('%d.%m.%Y')}: {e}")
        return None

    return rates if len(rates) == 2 else None


def is_rates_file_fresh(rates_file):
    """Проверяет, актуален ли файл курсов (до вчерашнего дня)"""
    if not os.path.exists(rates_file):
        return False

    try:
        df = pd.read_csv(rates_file)
        df['date'] = pd.to_datetime(df['date'])
        last_date = df['date'].max().date()
        yesterday = (datetime.now() - timedelta(days=1)).date()
        return last_date >= yesterday
    except Exception:
        return False


def download_cbr_rates_full(start_date='2024-01-01', end_date=None):
    """Полная загрузка курсов ЦБ РФ за период"""

    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    rates_data = []

    log_message(f"⏳ Загрузка курсов ЦБ РФ за период {start_date} - {end_date}...")

    for i, date in enumerate(dates):
        if i % 50 == 0:
            log_message(f"📆 Обработано {i}/{len(dates)} дат...")

        rates = get_cbr_rates_for_date(date)
        if rates:
            rates_data.append({'date': date, 'USD': rates['USD'], 'EUR': rates['EUR']})
        else:
            log_message(f"⚠️ Пропущена дата {date.strftime('%d.%m.%Y')} (нет данных)")
        time.sleep(0.2)

    df_currency_rate = pd.DataFrame(rates_data)

    if not df_currency_rate.empty:
        os.makedirs(os.path.dirname(RATES_FILE), exist_ok=True)
        df_currency_rate.to_csv(RATES_FILE, index=False)
        log_message(f"✅ Курсы сохранены в: {RATES_FILE}")
        log_message(f"📊 Загружено {len(df_currency_rate)} записей")
        return df_currency_rate
    else:
        log_message("❌ Не удалось загрузить данные")
        return None


def update_exchange_rates(rates_file=RATES_FILE):
    """Дозагружает курсы за недостающий период и сохраняет"""

    #  Проверка наличия файла
    if not os.path.exists(rates_file):
        log_message("⚠️ Файл курсов не найден. Загружаем всё с нуля...")
        df = download_cbr_rates_full()
        if df is not None:
            return {
                'status': 'success',
                'message': f'Курсы загружены до {df["date"].max().strftime("%d.%m.%Y")}',
                'data': df
            }
        return {'status': 'error', 'message': 'Не удалось загрузить курсы', 'data': None}

    #  Проверка актуальности файла
    if is_rates_file_fresh(rates_file):
        existing_df = pd.read_csv(rates_file)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_date = existing_df['date'].max()
        return {
            'status': 'up_to_date',
            'message': f'Курсы валют актуальны на {last_date.strftime("%d.%m.%Y")}',
            'data': existing_df
        }

    #  Дозагрузка недостающих данных
    try:
        existing_df = pd.read_csv(rates_file)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_date = existing_df['date'].max().date()
        yesterday = (datetime.now() - timedelta(days=1)).date()

        start_date = last_date + timedelta(days=1)
        end_date = yesterday
        dates_to_download = pd.date_range(start=start_date, end=end_date, freq='D')

        log_message(f" Дозагрузка {len(dates_to_download)} дней: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")

        new_rates_data = []

        for i, date in enumerate(dates_to_download):
            rates = get_cbr_rates_for_date(date)
            if rates:
                new_rates_data.append({'date': date, 'USD': rates['USD'], 'EUR': rates['EUR']})
            else:
                log_message(f"⚠️ Пропущена дата {date.strftime('%d.%m.%Y')}")
            time.sleep(0.2)

        if new_rates_data:
            new_df = pd.DataFrame(new_rates_data)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            updated_df.to_csv(rates_file, index=False)
            latest_date = updated_df['date'].max()
            log_message(f"✅ Файл обновлён! Добавлено {len(new_df)} новых записей.")
            return {
                'status': 'success',
                'message': f'Курсы валют обновлены до {latest_date.strftime("%d.%m.%Y")}',
                'data': updated_df
            }

        else:
            return {
                'status': 'partial',
                'message': f'Нет новых данных. Используются курсы на {last_date.strftime("%d.%m.%Y")}',
                'data': existing_df
            }

    except Exception as e:
        log_message(f"❌ Ошибка при обновлении: {e}")
        return {'status': 'error', 'message': f'Ошибка обновления: {e}', 'data': None}


# 🔁 Автоматический запуск при выполнении скрипта
if __name__ == "__main__":
    print("🚀 Запуск обновления курсов валют...")
    result = update_exchange_rates()

    if result["status"] in ("success", "up_to_date"):
        print(f"✅ {result['message']}")
    else:
        print(f"⚠️ {result['message']}")

  

