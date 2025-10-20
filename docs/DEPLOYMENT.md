#  Инструкция по развертыванию на PythonAnywhere

Пошаговое руководство по развертыванию Flask приложения для обработки данных о продажах круизов на PythonAnywhere.

---

##  Предварительные требования

- Аккаунт на [PythonAnywhere](https://www.pythonanywhere.com) (бесплатный план подходит)
- Аккаунт на GitHub (опционально, для клонирования репозитория)

---

##  Шаг 1: Регистрация на PythonAnywhere

1. Перейди на [pythonanywhere.com](https://www.pythonanywhere.com)
2. Нажми **"Pricing & signup"**
3. Выбери **"Create a Beginner account"** (бесплатный план)
4. Заполни форму регистрации
5. Подтверди email

---

## 📦 Шаг 2: Загрузка кода на PythonAnywhere

### Вариант A: Через GitHub (рекомендуется)

1. Открой вкладку **"Consoles"** → **"Bash"**
2. Клонируй репозиторий:
```
    cd ~
    git clone https://github.com/vulcan4ik/cruise-dashboard-app.git
    cd cruise-dashboard-app

```

### Вариант B: Загрузка через Files

1. Открой вкладку **"Files"**
2. Создай папку `cruise-dashboard-app`
3. Загрузи все файлы через кнопку **"Upload a file"**


## 🐍 Шаг 3: Настройка виртуального окружения
В Bash консоли выполни:
Создание виртуального окружения
```
cd ~/cruise-dashboard-app
python3.10 -m venv venv
```
Активация окружения
`source venv/bin/activate`

Установка зависимостей
`pip install -r requirements.txt`

##  Шаг 4: Настройка веб-приложения

### 4.1 Создание Web App

1. Открой вкладку **"Web"**
2. Нажми **"Add a new web app"**
3. Выбери домен: `ваш-username.pythonanywhere.com`
4. Выбери **"Manual configuration"** (не Flask wizard!)
5. Выбери **"Python 3.10"**

### 4.2 Настройка WSGI файла

1. На странице Web найди раздел **"Code"**
2. Нажми на ссылку **WSGI configuration file** (например: `/var/www/username_pythonanywhere_com_wsgi.py`)
3. Удали всё содержимое и вставь:
```
import sys
import os

# Путь к вашему проекту
project_path = '/home/ваш-username/dashboard-cruise-app'
if project_path not in sys.path:
    sys.path.insert(0, project_path)

# Путь к virtualenv
activate_this = os.path.join(project_path, 'venv/bin/activate_this.py')
with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

#  Меняем рабочую директорию
os.chdir(project_path)

#  Инициализация приложения с обновлением курсов
from currency_updater import update_exchange_rates
update_exchange_rates()

# Импортируем из app.py
from app import app as application
application.debug = False
```

**Важно:** Замени `ваш-username` на свой username!

4. Нажми **"Save"**

### 4.3 Настройка путей

На странице Web:

1. **Virtualenv:** укажи путь к виртуальному окружению:

/home/ваш-username/cruise-dashboard-app/venv

2. **Working directory:** укажи путь к проекту:

/home/ваш-username/cruise-dashboard-app

##  Шаг 5: Создание необходимых папок
В Bash консоли:
```
cd ~/cruise-dashboard-app
mkdir -p uploads results app_data
chmod 755 uploads results app_data
```

##  Шаг 6: Запуск приложения

1. На вкладке **"Web"** нажми большую зелёную кнопку **"Reload"**
2. Подожди 10-20 секунд
3. Открой ссылку вверху страницы: `https://ваш-username.pythonanywhere.com`

---

## 🐛 Устранение ошибок

### Где смотреть логи

На вкладке **"Web"** прокрути вниз до раздела **"Log files"**:

- **Error log** - ошибки приложения и вывод `print()`
- **Server log** - общие логи сервера
- **Access log** - логи запросов

### Частые проблемы

**Проблема:** "Internal Server Error"

**Решение:**
1. Проверь Error log
2. Убедись что все пути в WSGI файле правильные
3. Проверь что виртуальное окружение активно

**Проблема:** "ModuleNotFoundError"

**Решение:**
```
source ~/cruise-dashboard-app/venv/bin/activate
pip install -r requirements.txt
```

**Проблема:** Курсы валют не обновляются

**Решение:**
```
cd ~/cruise-dashboard-app
source venv/bin/activate
python currency_updater.py
```

## 📊 Мониторинг

- Бесплатный план PythonAnywhere ограничен 100,000 запросов в день
- Приложение "засыпает" если не используется 3+ месяцев
- Проверяй Error log регулярно
*Последнее обновление: Октябрь 2025*