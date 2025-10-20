from flask import Flask, request, render_template, redirect, url_for, flash, send_file, session
import os
from werkzeug.utils import secure_filename
import processsing
from datetime import datetime
import pandas as pd

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-me-to-random-string-12345'  # ВАЖНО: поменяйте на случайную строку
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['RESULTS_FOLDER'] = '/home/vulcan4ik/dashboard-cruise-app/results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

# Создаем папки
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

# Разрешенные расширения
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_currency_status():
    """Получает информацию о статусе курсов валют"""
    try:
        rates_file = '/home/vulcan4ik/dashboard-cruise-app/app_data/currency_rates_2024-2025.csv'

        if not os.path.exists(rates_file):
            return {
                'status': 'error',
                'message': 'Файл курсов валют не найден',
                'class': 'error'
            }

        # Читаем файл курсов
        rates_df = pd.read_csv(rates_file)
        rates_df['date'] = pd.to_datetime(rates_df['date'])

        # Получаем даты
        min_date = rates_df['date'].min()
        max_date = rates_df['date'].max()
        total_records = len(rates_df)

        # Проверяем актуальность
        today = pd.Timestamp.now().normalize()
        max_date = pd.Timestamp(max_date).normalize()
        days_old = (today - max_date).days

        if days_old <= 2:
            status_class = 'success'
            status_text = '✅ Актуальны'
        elif days_old <= 7:
            status_class = 'warning'
            status_text = '⚠️ Требуют обновления'
        else:
            status_class = 'error'
            status_text = '❌ Устарели'

        return {
            'status': status_class,
            'message': status_text,
            'min_date': min_date.strftime('%d.%m.%Y'),
            'max_date': max_date.strftime('%d.%m.%Y'),
            'total_records': total_records,
            'days_old': days_old,
            'class': status_class
        }

    except Exception as e:
        print(f"❌ Ошибка получения статуса курсов: {str(e)}")
        return {
            'status': 'error',
            'message': f'Ошибка: {str(e)}',
            'class': 'error'
        }

@app.route('/')
def index():
    """Главная страница"""
    currency_info = get_currency_status()
    return render_template('index.html', currency_info=currency_info)

@app.route('/download/<filename>')
def download_file(filename):
    """Скачивание обработанного файла"""
    try:
        # Базовая проверка безопасности
        if '/' in filename or '\\' in filename or not filename.endswith('.csv'):
            flash('Некорректное имя файла')
            return redirect(url_for('index'))

        file_path = os.path.join(app.config['RESULTS_FOLDER'], filename)

        # Проверяем существование файла
        if not os.path.exists(file_path):
            flash('Файл не найден')
            return redirect(url_for('index'))

        # Отправляем файл для скачивания
        return send_file(
            file_path,
            as_attachment=True,
            download_name=f"cruise_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mimetype='text/csv'
        )

    except Exception as e:
        flash(f'Ошибка скачивания: {str(e)}')
        return redirect(url_for('index'))

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    """Загрузка и обработка файла"""
    if request.method == 'GET':
        return redirect('/')

    if 'file' not in request.files:
        flash('Файл не выбран')
        return redirect(url_for('index'))

    file = request.files['file']

    if file.filename == '':
        flash('Файл не выбран')
        return redirect(url_for('index'))

    if not (file and allowed_file(file.filename)):
        flash('Разрешены только файлы Excel (.xlsx, .xls) и CSV')
        return redirect(url_for('index'))

    filepath = None
    try:
        # Сохраняем файл
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        print(f"📂 Файл сохранён: {filepath}")

        # Обрабатываем данные (ИСПРАВЛЕНО: принимаем 3 значения)
        df_processed, result_filename, stats = processsing.process_and_upload(filepath)

        print(f"✅ Обработка завершена")
        print(f"📊 Обработано строк: {len(df_processed)}")
        print(f"📁 Имя файла результата: {result_filename}")
        print(f"📊 Статистика: {stats}")

        flash('✅ Данные успешно обработаны!')
        flash(f'📊 Обработано строк: {len(df_processed)}')
        flash(f'💱 Конвертация валют выполнена')
        flash(f'📥 CSV файл готов к загрузке в DataLens')

        # Сохраняем статистику в session
        session['processing_stats'] = stats
        print(f"💾 Stats сохранены в session: {stats}")

        print(f"🔗 Редирект на success с filename={result_filename}")
        return redirect(url_for('success', filename=result_filename))

    except Exception as e:
        print(f"❌ Ошибка обработки: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'❌ Ошибка обработки: {str(e)}')
        return redirect(url_for('index'))

    finally:
        # Удаляем загруженный файл после обработки
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
                print(f"🗑️ Удален загруженный файл: {filename}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить файл: {str(e)}")

@app.route('/success')
def success():
    """Страница успешной обработки"""
    filename = request.args.get('filename')
    stats = session.get('processing_stats', {})

    print(f"📄 Success page - получен filename: {filename}")
    print(f"📊 Stats получены из session: {stats}")
    print(f"📊 original_rows: {stats.get('original_rows', 'Нет данных')}")

    if not filename:
        flash('Файл не найден')
        return redirect(url_for('index'))

    return render_template('success.html', filename=filename, stats=stats)

if __name__ == '__main__':
    app.run(debug=True)

