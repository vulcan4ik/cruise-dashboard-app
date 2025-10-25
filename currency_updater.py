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

    except requests.exceptions.ConnectionError as e:
        log_message(f"❌ Ошибка соединения на {date.strftime('%d.%m.%Y')}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        log_message(f"❌ Timeout на {date.strftime('%d.%m.%Y')}: {e}")
        return None
    except ET.ParseError as e:
        log_message(f"❌ Ошибка парсинга XML на {date.strftime('%d.%m.%Y')}: {e}")
        return None
    except Exception as e:
        log_message(f"❌ Ошибка на {date.strftime('%d.%m.%Y')}: {type(e).__name__}: {e}")
        return None

    return rates if len(rates) == 2 else None


def is_rates_file_fresh(rates_file):
    """Проверяет, актуален ли файл курсов """
    if not os.path.exists(rates_file):
        return False

    try:
        df = pd.read_csv(rates_file)
        df['date'] = pd.to_datetime(df['date'])
        last_date = df['date'].max().date()
        today = datetime.now().date()
        days_diff = (today - last_date).days
        
        if days_diff <= 1:
            log_message(f"✅ Курсы актуальны (последняя дата: {last_date}, задержка: {days_diff} дней)")
            return True
        else:
            log_message(f"⚠️ Курсы устарели (последняя дата: {last_date}, задержка: {days_diff} дней)")
            return False
    except Exception as e:
        log_message(f"⚠️ Ошибка проверки свежести курса: {e}")
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

    log_message(f"\n{'='*60}")
    log_message(f"🚀 НАЧАЛО ОБНОВЛЕНИЯ КУРСОВ ЦБ РФ - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_message(f"{'='*60}\n")

    # Проверка наличия файла
    if not os.path.exists(rates_file):
        log_message("⚠️ Файл курсов не найден. Загружаем всё с нуля...")
        df = download_cbr_rates_full()
        if df is not None:
            log_message(f"\n✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО: Загружено {len(df)} записей\n")
            return {
                'status': 'success',
                'message': f'Курсы загружены до {df["date"].max().strftime("%d.%m.%Y")}',
                'data': df
            }
        return {'status': 'error', 'message': 'Не удалось загрузить курсы', 'data': None}

    # Проверка актуальности файла
    if is_rates_file_fresh(rates_file):
        existing_df = pd.read_csv(rates_file)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_date = existing_df['date'].max()
        log_message(f"\n✅ ОБНОВЛЕНИЕ НЕ ТРЕБУЕТСЯ - Данные уже актуальны\n")
        return {
            'status': 'up_to_date',
            'message': f'Курсы валют актуальны на {last_date.strftime("%d.%m.%Y")}',
            'data': existing_df
        }

    # Дозагрузка недостающих данных
    try:
        existing_df = pd.read_csv(rates_file)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_date = existing_df['date'].max().date()
        
        # Загружаем с последней даты в файле до вчеры
        start_date = last_date + timedelta(days=1)
        end_date = (datetime.now() - timedelta(days=1)).date()

        if start_date > end_date:
            log_message(f"✅ Данные уже актуальны (последняя дата: {last_date})")
            log_message(f"\n✅ ОБНОВЛЕНИЕ НЕ ТРЕБУЕТСЯ\n")
            return {
                'status': 'up_to_date',
                'message': f'Курсы валют актуальны на {last_date.strftime("%d.%m.%Y")}',
                'data': existing_df
            }

        dates_to_download = pd.date_range(start=start_date, end=end_date, freq='D')
        log_message(f"📥 Дозагрузка {len(dates_to_download)} дней: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n")

        new_rates_data = []

        for i, date in enumerate(dates_to_download):
            rates = get_cbr_rates_for_date(date)
            if rates:
                new_rates_data.append({'date': date, 'USD': rates['USD'], 'EUR': rates['EUR']})
            
            time.sleep(0.1)

        if new_rates_data:
            new_df = pd.DataFrame(new_rates_data)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Удаляем дубликаты по дате
            updated_df = updated_df.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
            
            updated_df.to_csv(rates_file, index=False)
            latest_date = updated_df['date'].max()
            
            log_message(f"\n✅ Файл обновлён! Добавлено {len(new_df)} новых записей.")
            log_message(f"📊 Всего записей: {len(updated_df)}")
            log_message(f"📅 Период: {updated_df['date'].min().strftime('%d.%m.%Y')} - {latest_date.strftime('%d.%m.%Y')}")
            log_message(f"\n✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО\n")
            
            return {
                'status': 'success',
                'message': f'Курсы валют обновлены до {latest_date.strftime("%d.%m.%Y")}',
                'data': updated_df
            }
        else:
            log_message("\n⚠️ Не удалось загрузить новые данные")
            log_message(f"\n⚠️ ОБНОВЛЕНИЕ ЗАВЕРШЕНО С ПРЕДУПРЕЖДЕНИЕМ\n")
            return {
                'status': 'partial',
                'message': f'Нет новых данных. Используются курсы на {last_date.strftime("%d.%m.%Y")}',
                'data': existing_df
            }

    except Exception as e:
        log_message(f"\n❌ Ошибка при обновлении: {type(e).__name__}: {e}")
        import traceback
        log_message(f"Traceback: {traceback.format_exc()}")
        log_message(f"\n❌ ОБНОВЛЕНИЕ ЗАВЕРШЕНО С ОШИБКОЙ\n")
        return {'status': 'error', 'message': f'Ошибка обновления: {e}', 'data': None}


# 🔁 Автоматический запуск
if __name__ == "__main__":
    print("\n🚀 Запуск обновления курсов валют ЦБ РФ...\n")
    result = update_exchange_rates()

    if result["status"] in ("success", "up_to_date"):
        print(f"\n✅ {result['message']}\n")
    else:
        print(f"\n⚠️ {result['message']}\n")