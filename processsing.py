# processsing.py
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import re
import numpy as np
import random

# Глобальная переменная для кэширования курсов
_CURRENCY_RATES_CACHE = None


def get_currency_rates(rates_file='/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'):
    """Загружает курсы валют с кэшированием"""
    global _CURRENCY_RATES_CACHE

    if _CURRENCY_RATES_CACHE is not None:
        return _CURRENCY_RATES_CACHE

    try:
        rates_df = pd.read_csv(rates_file)
        rates_df['date'] = pd.to_datetime(rates_df['date'])
        _CURRENCY_RATES_CACHE = rates_df
        print(f"✅ Курсы валют загружены: {len(rates_df)} записей")
        print(f"📅 Период курсов: {rates_df['date'].min()} - {rates_df['date'].max()}")
        return rates_df
    except Exception as e:
        print(f"❌ Ошибка загрузки курсов валют: {e}")
        return None


def convert_to_rub(row, rates_df):
    """Конвертирует сумму в рубли с учетом курса ЦБ + 4.5%"""

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

        if target_currency in rate_row:
            rate = float(rate_row[target_currency])
            # Курс ЦБ + 4.5% наценка
            converted_amount = (amount * rate) * 1.045
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

    # Читаем файл
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)


    df = rename_columns(df)         #  Переименовываем столбцы
    df = clean_numeric_data(df)     #  Очищаем числовые данные
    df = clean_data(df)             #  Удаляем ненужные строки
    df = fill_missing_buyer_names(df)  # Заполняем пропуски в названии агентств
    df = generate_amount_data(df)   #  Генерируем пропущенные суммы
    df = generate_payment_data(df)  #  Генерируем оплаты
    df = enrich_data(df)            #  Обогащаем данными

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
    print(f" Исходный размер данных: {df.shape}")
    
    # Проверка наличия обязательных столбцов
    required_columns = ['voucher_id', 'voucher_status']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"⚠️ ВНИМАНИЕ: Отсутствуют обязательные столбцы: {missing_cols}")
        return df
    
    # Удаляем строки с удаленными/аннулированными путевками
    if 'voucher_status' in df.columns:
        df = df[~df['voucher_status'].isin(['Удален', 'Аннулирован', 'удален','аннулирован'])]
        print(f"🔍 Размер данных после удаления аннулированных/удаленных: {df.shape}")
    
    # Удаляем строки с пустым voucher_id
    if 'voucher_id' in df.columns:
        initial_count = len(df)
        non_empty_mask = (
            df['voucher_id'].notna() &
            (df['voucher_id'] != '') &
            (df['voucher_id'].astype(str).str.strip() != '')
        )
        df = df[non_empty_mask]
        removed_count = initial_count - len(df)
        if removed_count > 0:
            print(f"🗑️ Удалено строк с пустым voucher_id: {removed_count}")
    
    # НОВОЕ: Проверяем пропуски в важных полях
    important_fields = ['buyer_department', 'buyer_name', 'manager']
    for field in important_fields:
        if field in df.columns:
            missing_count = df[field].isna().sum()
            empty_count = (df[field] == '').sum()
            if missing_count > 0 or empty_count > 0:
                print(f"⚠️ Поле '{field}': {missing_count} NaN, {empty_count} пустых строк")
    
    print(f"📊 Итоговый размер данных: {df.shape}")
    return df
    # Заполняем пропуски
    # df = df.fillna({
    #      'amount_to_pay': 0,
    #      'payment': 0,
    #      'people': 1,
    #      'days': 0
    #  })


# Генерируем правдоподобные данные для пропусков в суммах к оплате
def generate_amount_data(df):
    # Анализируем существующие данные для реалистичной генерации
    if df['amount_to_pay'].notna().sum() > 0:
        # Если есть реальные данные, используем их распределение
        existing_amounts = df['amount_to_pay'].dropna()
        mean_amount = existing_amounts.mean()
        std_amount = existing_amounts.std()

        # Генерируем данные на основе реального распределения
        missing_mask = df['amount_to_pay'].isna()
        n_missing = missing_mask.sum()
        generated_amounts = np.random.normal(mean_amount, std_amount, n_missing)

        # Ограничиваем разумными значениями (не отрицательные, не слишком большие)
        generated_amounts = np.clip(generated_amounts, 1000, 500000)
        df.loc[missing_mask, 'amount_to_pay'] = generated_amounts.round(2)
    else:
        # Если нет реальных данных, генерируем на основе здравого смысла
        missing_mask = df['amount_to_pay'].isna()
        n_missing = missing_mask.sum()

        # Генерируем суммы в зависимости от количества дней и людей
        base_amounts = []
        for idx in df[missing_mask].index:
            days = df.loc[idx, 'days']
            people = df.loc[idx, 'people']
            if pd.notna(days) and pd.notna(people):
                # Примерная логика: базовый тариф + стоимость за день и человека
                base = 5000 + (days * people * 1500)
                # Добавляем случайность
                base *= random.uniform(0.8, 1.5)
            else:
                base = random.randint(10000, 200000)
            base_amounts.append(base)

        df.loc[missing_mask, 'amount_to_pay'] = [round(x, 2) for x in base_amounts]

    return df

# Генерируем данные для оплаты на основе сумм к оплате
def generate_payment_data(df):
    missing_payment_mask = df['payment'].isna()

    for idx in df[missing_payment_mask].index:
        amount = df.loc[idx, 'amount_to_pay']
        if pd.notna(amount):
            # Оплата обычно немного меньше или равна сумме к оплате
            # (могут быть скидки, частичные оплаты и т.д.)
            payment = amount * random.uniform(0.7, 1.0)
            df.loc[idx, 'payment'] = round(payment, 2)
        else:
            df.loc[idx, 'payment'] = round(random.uniform(5000, 150000), 2)

    return df


def extract_region(agency_name):
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
        'Омск': ['омск'],
        'Уфа': ['уфа'],
        'Волгоград': ['волгоград'],
        'Кемерово': ['кемерово'],
        'Орёл': ['орёл', 'орел'],
        'Липецк': ['липецк'],
        'Кострома': ['кострома'],
        'Якутск': ['якутск', 'yakutsk'],
        'Петрозаводск': ['петрозаводск'],
        'Южно-Сахалинск': ['южно-сахалинск'],
        'Химки': ['химки'],
        'Чехов': ['чехов'],
        'Одинцово': ['одинцово'],
        'Лида': ['лида'],
        'Владимир': ['владимир'],
        'Александров': ['александров'],
        'Старое Село': ['старое село'],
        'Люберцы': ['люберцы'],
        'Белгород': ['белгород'],
        'Великий Новгород': ['великий новгород'],
        'Мытищи': ['мытищи'],
        'Абакан': ['абакан'],
        'Оренбург': ['оренбург']
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

def fill_missing_buyer_names(df):
    """Заполняет пропуски в buyer_name: КЛИЕНТСКИЙ ЗАЛ или 'Не определен'"""
    
    # Проверяем наличие столбцов
    if 'buyer_department' not in df.columns or 'buyer_name' not in df.columns:
        return df
    
    #  Сначала заполняем КЛИЕНТСКИЙ ЗАЛ
    mask_client_hall = (
        (df['buyer_department'] == 'КЛИЕНТСКИЙ ЗАЛ') & 
        (df['buyer_name'].isna() | (df['buyer_name'] == ''))
    )
    count_client_hall = mask_client_hall.sum()
    df.loc[mask_client_hall, 'buyer_name'] = 'КЛИЕНТСКИЙ ЗАЛ'
    
    # Затем заполняем остальные пропуски
    mask_other = df['buyer_name'].isna() | (df['buyer_name'] == '')
    count_other = mask_other.sum()
    df.loc[mask_other, 'buyer_name'] = 'Не определен'
    
    # статистика
    if count_client_hall > 0:
        print(f" Заполнено 'КЛИЕНТСКИЙ ЗАЛ': {count_client_hall} записей")
    if count_other > 0:
        print(f" Заполнено 'Не определен': {count_other} записей")
    if count_client_hall == 0 and count_other == 0:
        print(" Пропусков в buyer_name не найдено")
    
    return df



def enrich_data(df):
    """Добавляем аналитические колонки"""

    # Преобразуем даты
    df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
    df['checkin_date'] = pd.to_datetime(df['checkin_date'], errors='coerce')

        # Конвертация валют в рубли
    print("💱 Конвертация валют в рубли...")
    rates_df = get_currency_rates()
    if rates_df is not None:
        df['amount_rub'] = df.apply(lambda row: convert_to_rub(row, rates_df), axis=1)
        print(f"✅ Конвертация завершена для {len(df)} записей")
    else:
        print("⚠️ Курсы валют не загружены, пропускаем конвертацию")
        df['amount_rub'] = df['amount_to_pay']  # Используем исходные суммы

     # Аналитические колонки
    if 'payment' in df.columns and 'amount_to_pay' in df.columns:
        df['payment_percentage'] = (df['payment'] / df['amount_to_pay'] * 100).round(2)
        # Защита от деления на ноль
        df['payment_percentage'] = df['payment_percentage'].replace([np.inf, -np.inf], 0)

    if 'checkin_date' in df.columns and 'creation_date' in df.columns:
        df['days_until_checkin'] = (df['checkin_date'] - df['creation_date']).dt.days
        df['days_until_checkin'] = df['days_until_checkin'].fillna(0)

    if 'country' in df.columns:

        cruise_agents = df.loc[df['country'].astype(str).str.lower().str.contains('круиз'), 'buyer_name'].unique()

        df['is_cruise_seller'] = df['buyer_name'].apply(lambda x: 1 if x in cruise_agents else 0)

    if 'creation_date' in df.columns:
        df['creation_month'] = df['creation_date'].dt.month

    # Извлекаем регион
    if 'buyer_name' in df.columns:
        df['region'] = df['buyer_name'].apply(extract_region)

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


# Функция для полного пайплайна
def process_and_upload(file_path, credentials_file='/home/vulcan4ik/dashboard-cruise-app/credentials.json'):
    """
    Полный пайплайн обработки и загрузки данных
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

    return processed_df, csv_filename

