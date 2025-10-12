# 🌊🚢cruise-dashboard-app

## web - версия:  https://vulcan4ik.pythonanywhere.com/

# 🚢 Cruise Sales Analytics Dashboard

Веб-приложение для автоматической обработки данных о продажах круизов с конвертацией валют по курсу ЦБ РФ и подготовкой данных для аналитических дашбордов.

## 🌊🚢 Описание проекта

Приложение предназначено для менеджеров и аналитиков туристического агентств, работающих с продажами круизов. Автоматизирует процесс обработки отчета о продажах путевок за выбранный период, конвертирует суммы в рубли по актуальным курсам ЦБ РФ и готовит данные для визуализации в Yandex DataLens.

## Основные возможности

-  **Загрузка файлов** - поддержка Excel (.xlsx, .xls) и CSV
-  **Автоматическая конвертация валют** - по курсу ЦБ РФ с учетом комиссии 4.5%
-  **Обновление курсов** - автоматическое при запуске на текущую дату
-  **Экспорт результатов** - готовый CSV для DataLens
-  **Веб-интерфейс** - удобная загрузка через drag-and-drop

## Технологии

- **Backend**: Python 3.12, Flask
- **Data Processing**: Pandas, NumPy
- **API Integration**: ЦБ РФ XML APIgit remote add origin https://github.com/ВАШ-USERNAME/cruise-analytics-dashboard.git
- **Frontend**: HTML5, CSS3, JavaScript
- **Deployment**: PythonAnywhere
- **Dashboard**: Yandex Datalens

## Установка и запуск

### Требования
- Python 3.8+
- pip

### web - версия:
- https://vulcan4ik.pythonanywhere.com/

### Локальная установка
#### 📂 Структура проекта

```
dashboard-cruise-app/
│
├── 📄 README.md                       # Описание проекта
├── 📄 requirements.txt                # Зависимости Python
├── 📄 .gitignore                      # Исключения для Git
│
├── 🐍 app.py                          # Главный файл Flask приложения
├── 🐍 processsing.py                  # Обработка и очистка данных
├── 🐍 currency_updater.py             # Обновление курсов валют 
│
├── 📁 templates/                      # HTML шаблоны
│   ├── index.html                     # Главная страница с загрузкой файлов
│   └── success.html                   # Страница успешной обработки
│
├── 📁 sample_data/                    # Примеры данных (обезличенные)
│   ├── sample_input.xlsx              # Пример входного файла
│   └── sample_output.csv              # Пример обработанного файла
│
├── 📁 app_data/
│   └── currency_rates_2024-2025.csv   # Курсы валют ЦБ РФ (2024-2025)
│
├── 📁 uploads/                        # Временное хранение загруженных файлов
├── 📁 results/                        # Обработанные файлы для скачивания
│
└── 📁 docs/
    └── DEPLOYMENT.md                  # Инструкция по развертыванию
```

1. Клонируйте репозиторий:
git clone https://github.com/vulcan4ik/cruise-dashboard-app.git
cd dashboard-cruise-app

2. Создайте виртуальное окружение:
python -m venv venv
source venv/bin/activate # Linux/Mac
venv\Scripts\activate # Windows

3. Установите зависимости:
pip install -r requirements.txt

4. Создайте необходимые папки:
mkdir -p uploads results app_data

5. поместите файл `currency_rates_2024-2025.csv` в директорию /home/<username>/dashboard-cruise-app/app_data/ чтобы уменьшить время на обновление курса валют при первом запуске

5. Запустите приложение:
python app.py

6. Откройте в браузере: `http://127.0.0.1:5000`


## Деплой на PythonAnywhere

Подробная инструкция в [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)


## Планы развития

- [ ] Автоматическая загрузка в DataLens 
- [ ] История загрузок файлов
- [ ] Поддержка дополнительных валют

## Автор

Alexey Kharchenko, junior Data Analyst
- GitHub: [@vulcan4ik](https://github.com/vulcan4ik)
- tg: https://t.me/vulcan4ik


