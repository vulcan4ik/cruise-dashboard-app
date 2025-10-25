# processsing.py
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import re
import numpy as np


# Глобальная переменная для кэширования курсов
_CURRENCY_RATES_CACHE = None

# Глобальная переменная для статистики обработки
PROCESSING_STATS = {}


def reset_stats():
    """Сбрасывает статистику обработки"""
    global PROCESSING_STATS
    PROCESSING_STATS = {
        'original_rows': 0,
        'original_cols': 0,
        'removed_duplicates': 0,
        'removed_cancelled': 0,
        'removed_empty_voucher': 0,
        'filled_buyer_name_client_hall': 0,
        'filled_buyer_name_undefined': 0,
        'converted_currency': 0,
        'extracted_regions': 0,
        'final_rows': 0,
        'final_cols': 0,
        'added_cols': []
    }


def get_currency_rates(rates_file='/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'):

    """Загружает курсы валют с кэшированием"""
    global _CURRENCY_RATES_CACHE

    if _CURRENCY_RATES_CACHE is not None:
        return _CURRENCY_RATES_CACHE

    try:
        # ← ДОБАВИТЬ ЭТУ ПРОВЕРКУ:
        if not os.path.exists(rates_file):
            print(f"❌ Файл курсов не найден: {rates_file}")
            return None

        rates_df = pd.read_csv(rates_file)
        rates_df['date'] = pd.to_datetime(rates_df['date'])
        _CURRENCY_RATES_CACHE = rates_df
        print(f"✅ Курсы валют загружены: {len(rates_df)} записей")
        print(f"📅 Период курсов: {rates_df['date'].min().date()} - {rates_df['date'].max().date()}")
        return rates_df
    except Exception as e:
        print(f"❌ Ошибка загрузки курсов валют: {e}")
        return None


def convert_to_rub(row, rates_df):
    """Конвертирует сумму в рубли с учетом курса ЦБ + 4.5%"""
    global PROCESSING_STATS

    # Проверка наличия суммы
    if pd.isna(row.get('amount_to_pay')) or row['amount_to_pay'] in [0, '', None]:
        return 0

    amount = float(row['amount_to_pay'])
    currency = str(row.get('currency', 'рб')).strip()

    # Маппинг валют
    currency_map = {
        'E': 'EUR',
        'EUR': 'EUR',
        '€': 'EUR',
        '$': 'USD',
        'USD': 'USD',
        'рб': 'RUB',
        'RUB': 'RUB',
        'руб': 'RUB'
    }

    target_currency = currency_map.get(currency, 'RUB')

    # Если рубли - возвращаем без изменений
    if target_currency == 'RUB':
        return round(amount, 2)

    # Для валюты нужна дата и курс
    creation_date = row.get('creation_date')

    if rates_df is None or pd.isna(creation_date):
        print(f"⚠️ Нет данных для конвертации: валюта={currency}, дата={creation_date}")
        return 0

    try:
        # Ищем ближайшую предыдущую дату с курсом
        creation_date = pd.to_datetime(creation_date)
        available_dates = rates_df[rates_df['date'] <= creation_date]

        if available_dates.empty:
            # Если дата раньше всех курсов - берем самый ранний курс
            rate_row = rates_df.iloc[0]
            print(f"⚠️ Дата {creation_date.date()} раньше доступных курсов, используем {rate_row['date'].date()}")
        else:
            # Берем последний доступный курс до даты создания
            rate_row = available_dates.iloc[-1]

        if target_currency in rate_row.index and pd.notna(rate_row[target_currency]):
            rate = float(rate_row[target_currency])
            # Курс ЦБ + 4.5% наценка
            converted_amount = (amount * rate) * 1.045
            PROCESSING_STATS['converted_currency'] += 1
            return round(converted_amount, 2)
        else:
            print(f"❌ Валюта {target_currency} не найдена в курсах")
            return 0

    except Exception as e:
        print(f"❌ Ошибка конвертации для строки: {e}")
        return 0


def clean_numeric_data(df):
    """Очищает числовые данные от запятых и других нечисловых символов"""

    def convert_numeric_value(x):
        if pd.isna(x) or x == '':
            return x

        str_val = str(x).strip()

        # Убираем запятые (разделители тысяч)
        cleaned = str_val.replace(',', '')

        try:
            return float(cleaned)
        except ValueError:
            # Если не получается преобразовать, оставляем как есть
            return x

    # Применяем ко всем столбцам, которые выглядят как числовые
    for column in df.columns:
        # Проверяем, есть ли в столбце строки с запятыми
        has_commas = df[column].apply(lambda x: ',' in str(x) if pd.notna(x) else False).any()

        if has_commas:
            df[column] = df[column].apply(convert_numeric_value)

    return df


def process_data(file_path):
    """Основная функция обработки данных"""
    global PROCESSING_STATS
    reset_stats()

    # Читаем файл
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Сохраняем исходную статистику
    PROCESSING_STATS['original_rows'] = len(df)
    PROCESSING_STATS['original_cols'] = len(df.columns)

    print(f"📂 Исходный файл: {len(df)} строк, {len(df.columns)} столбцов")

    # ПЕРВЫМ ДЕЛОМ - переименовываем столбцы
    df = rename_columns(df)

    # Затем очищаем числовые данные
    df = clean_numeric_data(df)

    # Обработка данных (БЕЗ генерации)
    df = clean_data(df)
    df = fill_missing_buyer_names(df)
    df = enrich_data(df)

    # Финальная статистика
    PROCESSING_STATS['final_rows'] = len(df)
    PROCESSING_STATS['final_cols'] = len(df.columns)
    PROCESSING_STATS['added_cols'] = ['amount_rub', 'region', 'is_cruise_seller', 'payment_percentage', 'days_until_checkin', 'creation_month']

    print(f"✅ Обработка завершена: {len(df)} строк, {len(df.columns)} столбцов")

    return df


def rename_columns(df):
    """Переименование столбцов согласно словарю"""
    rename_dict = {
        'Путевка': 'voucher_id',
        'Страна': 'country',
        'Дата создания': 'creation_date',
        'Дата заезда': 'checkin_date',
        'Дней': 'days',
        'Человек': 'people',
        'Статус путевки': 'voucher_status',
        'Внутренний статус': 'internal_status',
        'Валюта': 'currency',
        'Сумма к оплате': 'amount_to_pay',
        'Оплата': 'payment',
        'Название тура': 'tour_name',
        'Покупатель: Ответственное подразделение': 'buyer_department',
        'Покупатель: Наименование': 'buyer_name',
        'Покупатель: Категория ТА': 'buyer_category',
        'Создатель': 'creator',
        'Ведущий менеджер': 'manager'
    }

    # переименовываем - pandas автоматически игнорирует отсутствующие столбцы
    df = df.rename(columns=rename_dict)

    # Оставляем только столбцы из словаря (те которые существуют после переименования)
    final_columns = [v for v in rename_dict.values() if v in df.columns]
    df = df[final_columns]

    print(f"✅ Переименовано столбцов: {len(final_columns)}")
    print(f"📋 Итоговые столбцы: {list(df.columns)}")

    return df


def clean_data(df):
    """Очистка данных"""
    global PROCESSING_STATS

    print(f"🔍 Исходный размер данных: {df.shape}")

    # Удаляем строки с удаленными/аннулированными путевками
    initial_count = len(df)
    df = df[~df['voucher_status'].isin(['Удален', 'Аннулирован', 'удален', 'аннулирован'])]
    PROCESSING_STATS['removed_cancelled'] = initial_count - len(df)
    print(f"🔍 Размер данных после удаления аннулированных/удаленных: {df.shape}")

    # Удаляем строки с пустым voucher_id (включая последние строки)
    if 'voucher_id' in df.columns:
        initial_count = len(df)
        # Создаем маску для непустых voucher_id
        non_empty_mask = (
            df['voucher_id'].notna() &
            (df['voucher_id'] != '') &
            (df['voucher_id'].astype(str).str.strip() != '')
        )
        df = df[non_empty_mask]
        removed_count = initial_count - len(df)
        PROCESSING_STATS['removed_empty_voucher'] = removed_count
        if removed_count > 0:
            print(f"🗑️ Удалено строк с пустым voucher_id: {removed_count}")

    print(f"📊 Итоговый размер данных: {df.shape}")
    return df


def fill_missing_buyer_names(df):
    """Заполняет пропуски в buyer_name: КЛИЕНТСКИЙ ЗАЛ или 'Не определен'"""
    global PROCESSING_STATS

    # Проверяем наличие столбцов
    if 'buyer_department' not in df.columns or 'buyer_name' not in df.columns:
        return df

    # Сначала заполняем КЛИЕНТСКИЙ ЗАЛ
    mask_client_hall = (
        (df['buyer_department'] == 'КЛИЕНТСКИЙ ЗАЛ') &
        (df['buyer_name'].isna() | (df['buyer_name'] == ''))
    )
    count_client_hall = mask_client_hall.sum()
    df.loc[mask_client_hall, 'buyer_name'] = 'КЛИЕНТСКИЙ ЗАЛ'
    PROCESSING_STATS['filled_buyer_name_client_hall'] = count_client_hall

    # Затем заполняем остальные пропуски
    mask_other = df['buyer_name'].isna() | (df['buyer_name'] == '')
    count_other = mask_other.sum()
    df.loc[mask_other, 'buyer_name'] = 'Не определен'
    PROCESSING_STATS['filled_buyer_name_undefined'] = count_other

    # Выводим статистику
    if count_client_hall > 0:
        print(f"✅ Заполнено 'КЛИЕНТСКИЙ ЗАЛ': {count_client_hall} записей")
    if count_other > 0:
        print(f"✅ Заполнено 'Не определен': {count_other} записей")

    return df


def extract_region(agency_name):
    """Извлекает регион из названия агентства"""
    if not isinstance(agency_name, str) or agency_name.strip() == '' or agency_name.lower() in ['n/a', 'nan', 'none']:
        return 'Не указано'

    text = agency_name.strip()

    # Расширенный список городов с приоритетом
    known_cities = {
        'Москва': ['москва', 'мск', 'moscow'],
        'Санкт-Петербург': ['санкт-петербург', 'спб', 'питер', 'st. petersburg', 'petersburg'],
        'Новосибирск': ['новосибирск', 'новосиб'],
        'Екатеринбург': ['екатеринбург', 'екб'],
        'Казань': ['казань', 'kazan'],
        'Краснодар': ['краснодар'],
        'Пермь': ['пермь', 'perm'],
        'Ростов-на-Дону': ['ростов-на-дону', 'ростов'],
        'Тюмень': ['тюмень'],
        'Барнаул': ['барнаул'],
        'Красноярск': ['красноярск'],
        'Владивосток': ['владивосток', 'vladivostok'],
        'Самара': ['самара', 'samara'],
        'Минск': ['минск', 'minsk'],
        'Бишкек': ['бишкек', 'bishkek'],
        'Астана': ['астана', 'astana'],
        'Сочи': ['сочи', 'sochi'],
        'Ярославль': ['ярославль'],
        'Воронеж': ['воронеж'],
        'Иркутск': ['иркутск'],
        'Хабаровск': ['хабаровск'],
        'Ставрополь': ['ставрополь'],
        'Челябинск': ['челябинск'],
        'Новороссийск': ['новороссийск'],
        'Томск': ['томск'],
        'Киев': ['киев', 'kyiv'],
        'Ташкент': ['ташкент', 'tashkent'],
        'Ереван': ['ереван', 'yerevan'],
        'Баку': ['баку', 'baku'],
        'Алматы': ['алматы', 'almaty'],
    }

    # Сначала ищем известные города в любом месте строки
    for city, variants in known_cities.items():
        for variant in variants:
            if variant in text.lower():
                return city

    # Если город не найден, парсим структуру (только последний элемент после запятой)
    parts = [p.strip() for p in re.split(r'[,;]', text) if p.strip()]

    if len(parts) > 1:
        # Берем последнюю часть (обычно там город)
        last_part = parts[-1]

        # Расширенный список стоп-слов
        stop_words = {
            'ИП', 'ТРЕВЕЛ', 'ГРУПП', 'ТУР', 'ВОЯЖ', 'КОРАЛ', 'АНЕКС', 'PAC', 'ПАК',
            'TRAVEL', 'GROUP', 'ООО', 'ЗАО', 'АО', 'LTD', 'CORP', 'COMPANY', 'CLUB',
            'м.', 'ул.', 'пр.', 'бульвар', 'проспект', 'улица', 'ЦЕНТР', 'ОФИС', 'ОТДЕЛ',
            'ФИЛИАЛ', 'АГЕНТСТВО', 'БЮРО', 'СЕТЬ', 'КОМПАНИЯ', 'EXPERT', 'EXPERTS',
            'WORLD', 'INTERNATIONAL', 'SERVICE', 'SERVICES', 'КРУКЛАБ', 'АЛЛИНТРЭВЕЛ',
            'ГЕРМЕС', 'САНЭКСПРЕСС-ГП', 'МА МИЛЬЯНА', 'КРУГОЗОР', 'ПРАЙМ', 'ЭДЕМ-СЕРВИС',
            'БУТИК ПУТЕШЕСТВИЙ', 'АП АРФА', 'КРАСКИ МИРА', 'БОНЖУР', 'МЕРИДИАН',
            'ДИРЕКТОРИУМ', 'РЕГИОН', 'ВОЛГА', 'СИБИРЬ', 'УРАЛ', 'ДАЛЬНИЙ ВОСТОК'
        }

        # Проверяем, что это не стоп-слово и похоже на географическое название
        if (not any(stop_word in last_part.upper() for stop_word in stop_words) and
            len(last_part) >= 3 and
            not last_part.isdigit() and
            re.match(r'^[А-ЯЁа-яёA-Za-z\- ]+$', last_part)):

            # Дополнительная проверка - не должно быть общих слов
            common_words = {'ТУРИЗМ', 'ОТДЫХ', 'ПУТЕШЕСТВИЙ', 'ТУРОВ', 'ВОЯЖ', 'ТРЕВЕЛ'}
            if not any(word in last_part.upper() for word in common_words):
                return last_part.strip()

    return 'Другой'


def enrich_data(df):

    global PROCESSING_STATS

    print(f"🔄 Начало обогащения данных...")


    # Преобразуем даты в datetime формат
    if 'creation_date' in df.columns:
        df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

    if 'checkin_date' in df.columns:
        df['checkin_date'] = pd.to_datetime(df['checkin_date'], errors='coerce')

    # Загружаем курсы перед конвертацией
    rates_df = get_currency_rates()

    if rates_df is None:
        print(f"⚠️  Курсы валют не загружены, конвертация пропущена")
        df['amount_rub'] = 0
    else:
        # передаём rates_df в функцию
        print(f"💱 Начало конвертации валют...")
        df['amount_rub'] = df.apply(
            lambda row: convert_to_rub(row, rates_df),
            axis=1
        )
        print(f"✅ Конвертировано строк: {PROCESSING_STATS['converted_currency']}")

    # Извлечение региона из страны (если нужно)
    if 'country' in df.columns:
        print(f"🌍 Извлечение регионов...")
        df['region'] = df['country'].apply(extract_region)
        PROCESSING_STATS['extracted_regions'] = df['region'].notna().sum()
    else:
        df['region'] = 'Неизвестно'

    # Проверка, круизная ли путевка
    df['is_cruise_seller'] = False
    if 'tour_name' in df.columns:
        df['is_cruise_seller'] = df['tour_name'].str.contains('круиз|cruise', case=False, na=False)

    # Процент оплаты
    df['payment_percentage'] = 0.0
    if 'payment' in df.columns and 'amount_rub' in df.columns:
        mask = df['amount_rub'] > 0
        df.loc[mask, 'payment_percentage'] = (
            (df.loc[mask, 'payment'] / df.loc[mask, 'amount_rub'] * 100).round(2)
        )

    # Дни до заезда
    if 'checkin_date' in df.columns:
        df['checkin_date'] = pd.to_datetime(df['checkin_date'], errors='coerce')
        df['days_until_checkin'] = (df['checkin_date'] - pd.Timestamp.now()).dt.days
    else:
        df['days_until_checkin'] = 0

    # Месяц создания
    if 'creation_date' in df.columns:
        df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
        df['creation_month'] = df['creation_date'].dt.strftime('%Y-%m')
    else:
        df['creation_month'] = 'Неизвестно'

    print(f"✅ Обогащение данных завершено")
    return df


def upload_to_sheets(df, credentials_file='/home/vulcan4ik/dashboard-cruise-app/credentials.json', spreadsheet_name=None):
    """
    Загрузка данных в Google Sheets с fallback на локальное сохранение
    ВСЕГДА сохраняет CSV файл для скачивания
    """
    # СНАЧАЛА всегда сохраняем локально
    csv_filename = save_data_locally(df)

    # ПОТОМ пробуем загрузить в Google Sheets (опционально)
    print("\n📤 Попытка загрузки в Google Sheets...")

    try:
        # Проверяем существование файла credentials
        if not os.path.exists(credentials_file):
            print(f"⚠️ Файл credentials не найден: {credentials_file}")
            print("⚠️ Пропускаем загрузку в Google Sheets")
            return csv_filename

        print(f"🔑 Используется credentials файл: {credentials_file}")

        # Настройка подключения к Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
        client = gspread.authorize(creds)

        # Используем постоянное имя таблицы
        SPREADSHEET_NAME = "Cruise_Analytics_Dashboard"

        try:
            # Пробуем открыть существующую таблицу
            spreadsheet = client.open(SPREADSHEET_NAME)
            print(f"📊 Используем существующую таблицу: {SPREADSHEET_NAME}")
        except gspread.SpreadsheetNotFound:
            # Если таблицы нет, создаем новую
            spreadsheet = client.create(SPREADSHEET_NAME)
            print(f"📊 Создана новая таблица: {SPREADSHEET_NAME}")

        worksheet = spreadsheet.get_worksheet(0)

        # Загружаем данные
        data = [df.columns.tolist()] + df.values.tolist()

        # Очищаем лист перед загрузкой новых данных
        worksheet.clear()

        # Загружаем данные
        worksheet.update('A1', data)

        # Открываем доступ
        spreadsheet.share(None, perm_type='anyone', role='reader')

        print(f"✅ Данные успешно загружены в Google Sheets!")
        print(f"📊 Ссылка: {spreadsheet.url}")

        # ВАЖНО: Всё равно возвращаем имя CSV файла
        return csv_filename

    except Exception as e:
        print(f"⚠️ Ошибка загрузки в Google Sheets: {str(e)}")
        print("⚠️ Продолжаем работу с локальным CSV файлом")
        # Возвращаем имя файла в любом случае
        return csv_filename


def save_data_locally(df):
    """Сохранение данных локально"""
    try:
        # Создаем папку для результатов если не существует
        results_dir = '/home/vulcan4ik/dashboard-cruise-app/results'
        os.makedirs(results_dir, exist_ok=True)

        # Сохраняем DataFrame
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"processed_{timestamp}.csv"
        csv_path = os.path.join(results_dir, csv_filename)

        # Сохраняем с кодировкой UTF-8 с BOM для корректного открытия в Excel
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')

        print(f"✅ Данные успешно сохранены локально!")
        print(f"📊 Файл: {csv_path}")
        print(f"📦 Размер: {len(df)} строк, {len(df.columns)} столбцов")

        # Возвращаем только имя файла для маршрута /download/
        return csv_filename

    except Exception as e:
        print(f"❌ Ошибка локального сохранения: {str(e)}")
        raise


def convert_stats_to_json_serializable(stats):
    """Конвертирует numpy типы в обычные Python типы для JSON"""
    import numpy as np

    result = {}
    for key, value in stats.items():
        if isinstance(value, (np.integer, np.int64, np.int32)):
            result[key] = int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)):
            result[key] = float(value)
        elif isinstance(value, list):
            result[key] = value
        else:
            result[key] = value
    return result


def process_and_upload(file_path, credentials_file='/home/vulcan4ik/dashboard-cruise-app/credentials.json'):
    """
    Полный пайплайн обработки и загрузки данных
    Возвращает: (df, filename, stats)
    """
    print("\n" + "="*50)
    print("🚀 НАЧАЛО ОБРАБОТКИ ДАННЫХ")
    print("="*50 + "\n")

    # Обработка данных
    processed_df = process_data(file_path)

    print("\n" + "="*50)
    print("📤 СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
    print("="*50 + "\n")

    # Сохраняем локально и пробуем загрузить в Google Sheets
    csv_filename = upload_to_sheets(processed_df, credentials_file)

    print("\n" + "="*50)
    print("✅ ОБРАБОТКА ЗАВЕРШЕНА")
    print("="*50 + "\n")

    # Конвертируем stats в JSON-совместимый формат
    stats_clean = convert_stats_to_json_serializable(PROCESSING_STATS)

    print(f"📊 Финальная статистика (очищенная): {stats_clean}")

    # Возвращаем данные + статистику
    return processed_df, csv_filename, stats_clean




