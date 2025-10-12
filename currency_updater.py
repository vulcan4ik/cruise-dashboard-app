# currency_updater.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from xml.etree import ElementTree as ET
import os

def get_cbr_rate(date, currency_code):
    """Получение курса ЦБ на определенную дату"""
    url = "http://www.cbr.ru/scripts/XML_daily.asp"
    params = {'date_req': date.strftime('%d/%m/%Y')}

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for valute in root.findall('Valute'):
                if valute.find('CharCode').text == currency_code:
                    value = valute.find('Value').text
                    return float(value.replace(',', '.'))
    except Exception as e:
        print(f"Ошибка получения курса {currency_code} на {date}: {e}")

    return None


def download_cbr_rates_full(start_date='2024-01-01', end_date=None):
    """Полная загрузка курсов ЦБ РФ за период"""

    # ИСПРАВЛЕНИЕ: если end_date не указан, используем вчерашний день
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    rates_data = []

    print(f"⏳ Загрузка курсов ЦБ РФ за период {start_date} - {end_date}...")

    for i, date in enumerate(dates):
        if i % 50 == 0:
            print(f"Обработано {i}/{len(dates)} дат...")

        usd_rate = get_cbr_rate(date, 'USD')
        eur_rate = get_cbr_rate(date, 'EUR')

        if usd_rate and eur_rate:
            rates_data.append({
                'date': date,
                'USD': usd_rate,
                'EUR': eur_rate
            })

        time.sleep(0.2)  # Пауза чтобы не нагружать API ЦБ

    df_currency_rate = pd.DataFrame(rates_data)

    if not df_currency_rate.empty:
        filename = '/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df_currency_rate.to_csv(filename, index=False)

        print(f"✅ Реальные курсы сохранены в: {filename}")
        print(f"📊 Загружено {len(df_currency_rate)} записей")
        print(f"📅 Период: {df_currency_rate['date'].min()} - {df_currency_rate['date'].max()}")

        return df_currency_rate
    else:
        print("❌ Не удалось загрузить данные")
        return None


def update_exchange_rates(rates_file='/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'):
    """Дозагружает курсы за недостающий период с детальным статусом"""

    try:
        existing_df = pd.read_csv(rates_file)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        print(f"✅ Загружено существующих записей: {len(existing_df)}")

    except FileNotFoundError:
        print("❌ Файл курсов не найден. Создаем новый...")
        result = download_cbr_rates_full()

        if result is not None:
            return {
                'status': 'success',
                'message': f'Курсы валют успешно загружены на дату: {result["date"].max().strftime("%d.%m.%Y")}',
                'data': result
            }
        else:
            return {
                'status': 'error',
                'message': 'Не удалось загрузить курсы валют. Конвертация будет недоступна.',
                'data': None
            }

    except Exception as e:
        return {
            'status': 'error',
            'message': f'Ошибка загрузки файла курсов: {e}. Конвертация будет недоступна.',
            'data': None
        }

    last_date = existing_df['date'].max()
    current_date = datetime.now().date()
    yesterday = (datetime.now() - timedelta(days=1)).date()
    last_date_only = last_date.date()

    print(f"📅 Последняя дата в файле: {last_date.strftime('%d.%m.%Y')}")
    print(f"📅 Текущая дата: {current_date.strftime('%d.%m.%Y')}")

    # ИСПРАВЛЕНИЕ: проверяем актуальность до вчерашнего дня
    # (сегодняшние курсы могут быть еще не опубликованы)
    if last_date_only >= yesterday:
        return {
            'status': 'up_to_date',
            'message': f'Курсы валют актуальны на дату: {last_date.strftime("%d.%m.%Y")}',
            'data': existing_df
        }

    # Нужно дозагрузить - ИСПРАВЛЕНИЕ: только до вчера
    start_date = last_date + timedelta(days=1)
    end_date = datetime.now() - timedelta(days=1)  # До вчера, не до сегодня

    dates_to_download = pd.date_range(start=start_date, end=end_date, freq='D')

    print(f"📥 Попытка дозагрузки {len(dates_to_download)} дней: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")

    new_rates_data = []

    for i, date in enumerate(dates_to_download):
        if i % 20 == 0:
            print(f"⏳ Дозагружено {i}/{len(dates_to_download)}...")

        usd_rate = get_cbr_rate(date, 'USD')
        eur_rate = get_cbr_rate(date, 'EUR')

        if usd_rate and eur_rate:
            new_rates_data.append({
                'date': date,
                'USD': usd_rate,
                'EUR': eur_rate
            })

        time.sleep(0.2)

    # Если удалось загрузить новые данные
    if new_rates_data:
        new_df = pd.DataFrame(new_rates_data)
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)

        try:
            updated_df.to_csv(rates_file, index=False)
            latest_date = updated_df['date'].max()

            print(f"✅ Файл обновлен!")
            print(f"📈 Добавлено новых записей: {len(new_df)}")
            print(f"📊 Всего записей: {len(updated_df)}")

            return {
                'status': 'success',
                'message': f'Курсы валют обновлены на дату: {latest_date.strftime("%d.%m.%Y")}',
                'data': updated_df
            }

        except Exception as e:
            return {
                'status': 'error',
                'message': f'Не удалось сохранить обновленные курсы. Расчеты будут использовать данные на {last_date.strftime("%d.%m.%Y")}',
                'data': existing_df
            }

    else:
        # Не удалось загрузить новые данные
        return {
            'status': 'partial',
            'message': f'Не удалось обновить курсы валют. Расчеты будут использовать последние доступные данные на {last_date.strftime("%d.%m.%Y")}',
            'data': existing_df
        }


if __name__ == "__main__":
    update_exchange_rates()

