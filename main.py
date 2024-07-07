import psycopg2
import time
import re
import html
from collections import Counter
from datetime import datetime
import logging
import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_LENGTH = 3000
DATABASE_URL = "postgresql://user:password@db:5432/vacancies_db"
POSTGRES_URL = "postgresql://user:password@db:5432/postgres"
TOKEN = ''
bot = telebot.TeleBot(TOKEN)
vacancy_instances = {}
last_message_id = {}


class Vacancy:
    tgid = 0
    title = ""
    city = ""
    salary = ""
    experience = ""
    schedule = ""

    def __init__(self, tgid=None, title=None, city=None, salary=None,
                 exp=None, schedule=None):
        self.tgid = tgid
        self.title = title
        self.city = city
        self.salary = salary
        self.experience = exp
        self.schedule = schedule

    def send_aform(self):
        text = "Все введено корректно?" \
               f"\nНазвание вакансии: {self.title}" \
               f"\nГород: {self.city}" \
               f"\nЗаработная плата: {self.salary}" \
               f"\nОпыт работы: {self.experience}" \
               f"\nТип занятости: {self.schedule}"
        return text


def create_database():
    try:
        connection = psycopg2.connect(POSTGRES_URL)
        connection.autocommit = True
        cur1 = connection.cursor()
        cur1.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'vacancies_db'")
        exists = cur1.fetchone()

        if not exists:
            cur1.execute('CREATE DATABASE vacancies_db')
            print("Database 'vacancies_db' created successfully.")
        else:
            print("Database 'vacancies_db' already exists.")
        cur1.close()
        connection.close()

    except psycopg2.Error as error:
        print(f"Error creating database: {error}")
        logger.error(f'Error creating database: {error}')


def create_table():
    try:
        connection = psycopg2.connect(DATABASE_URL)
        connection.autocommit = True
        cur = connection.cursor()
        cur.execute('''
                CREATE TABLE IF NOT EXISTS vacancies (
                    tgid INTEGER,
                    vacancy_id INTEGER PRIMARY KEY,
                    vacancy_url VARCHAR(128),
                    name VARCHAR(128),
                    employer_name VARCHAR(128),
                    salary_from INTEGER,
                    salary_to INTEGER,
                    currency VARCHAR(4),
                    city VARCHAR(64),
                    requirement VARCHAR(204),
                    responsibility VARCHAR(204),
                    published_at TIMESTAMP
                )
            ''')
        cur.close()
        connection.close()
        print("Table 'vacancies' created successfully")
    except psycopg2.Error as error:
        print(f"Error creating table: {error}")
        logger.error(f'Error creating table: {error}')


def text_change(text):
    try:
        text = text.replace("highlighttext", '')
        text = text.replace("&lt;&gt;", '')
        text = text.replace("&lt;/&gt;", '')
    except Exception as error:
        # Ошибка не критична
        pass
    return text


def parsing_table(vacancies, tgid):
    try:
        def extract_vacancy_data(item):
            def truncate_text(text, max_length=201):
                if len(text) > max_length:
                    return text[:max_length] + '...'
                return text

            return {
                'tgid': tgid,
                'vacancy_id': item['id'],
                'vacancy_url': item['alternate_url'],
                'name': item['name'],
                'employer_name': item['employer']['name'] if item.get('employer') else None,
                'salary_from': item['salary']['from'] if item.get('salary') else None,
                'salary_to': item['salary']['to'] if item.get('salary') else None,
                'currency': item['salary']['currency'] if item.get('salary') else None,
                'city': item['area']['name'] if item.get('area') else None,
                'requirement': truncate_text(item['snippet']['requirement']) if item.get('snippet') and item[
                    'snippet'].get('requirement') else None,
                'responsibility': truncate_text(item['snippet']['responsibility']) if item.get('snippet') and item[
                    'snippet'].get('responsibility') else None,
                'published_at': item['published_at']
            }

        number_v = len(vacancies)
        if number_v == 0:
            return None
        sum_v = 0
        min_salary = float('inf')
        max_salary = float('-inf')
        all_requirements = []
        all_responsibilities = []
        data_list = []

        for item in vacancies:
            data = extract_vacancy_data(item)
            data_list.append(data)
            if data['salary_from'] and data['salary_to']:
                sum_v += (data['salary_from'] + data['salary_to']) // 2
                max_salary = max(max_salary, data['salary_to'])
                min_salary = min(min_salary, data['salary_from'])
            elif data['salary_from']:
                sum_v += data['salary_from']
                max_salary = max(max_salary, data['salary_from'])
                min_salary = min(min_salary, data['salary_from'])
            elif data['salary_to']:
                sum_v += data['salary_to']
                max_salary = max(max_salary, data['salary_to'])
                min_salary = min(min_salary, data['salary_to'])
            if data['requirement']:
                all_requirements.extend(re.findall(r'\w+', data['requirement'].lower()))
            if data['responsibility']:
                all_responsibilities.extend(re.findall(r'\w+', data['responsibility'].lower()))

        cursor.executemany('''
            INSERT INTO vacancies (tgid, vacancy_id, vacancy_url, name, employer_name, salary_from, salary_to, currency, city, requirement, responsibility, published_at)
            VALUES (%(tgid)s, %(vacancy_id)s, %(vacancy_url)s, %(name)s, %(employer_name)s, %(salary_from)s, %(salary_to)s, %(currency)s, %(city)s, %(requirement)s, %(responsibility)s, %(published_at)s)
            ON CONFLICT (vacancy_id) DO UPDATE SET
                tgid = EXCLUDED.tgid,
                vacancy_url = EXCLUDED.vacancy_url,
                name = EXCLUDED.name,
                employer_name = EXCLUDED.employer_name,
                salary_from = EXCLUDED.salary_from,
                salary_to = EXCLUDED.salary_to,
                currency = EXCLUDED.currency,
                city = EXCLUDED.city,
                requirement = EXCLUDED.requirement,
                responsibility = EXCLUDED.responsibility,
                published_at = EXCLUDED.published_at
        ''', data_list)

        conn.commit()

        sum_v = sum_v // number_v if number_v > 0 and sum_v > 0 else "Не указана"

        def get_top_words(words, top_n=10, min_length=3):
            stop_words = {'highlighttext', 'для', 'что', 'как', 'это'}
            return [word for word, count in
                    Counter(word for word in words if len(word) >= min_length and word not in stop_words).most_common(
                        top_n)]

        top_requirement_words = get_top_words(all_requirements)
        top_responsibility_words = get_top_words(all_responsibilities)

        if min_salary == float('inf'):
            min_salary = "Не указано"
        if max_salary == float('-inf'):
            max_salary = "Не указано"

        text = f"Всего найдено {number_v} вакансий, со средней зарплатой {sum_v}. Минимальная зарплата: {min_salary}, максимальная зарплата: {max_salary}\n" \
               f"<b>Наиболее часто встречающиеся слова в требованиях к соискателю:</b> {', '.join(top_requirement_words)}\n" \
               f"<b>Наиболее часто встречающиеся слова в обязанностях:</b> {', '.join(top_responsibility_words)}\n"

        for vacancy in data_list[:5]:
            formatted_date = datetime.fromisoformat(vacancy['published_at']).strftime('%Y-%m-%d %H:%M:%S')
            salary = "Не указана"
            if vacancy['salary_from'] or vacancy['salary_to']:
                salary = f"от {vacancy['salary_from'] or 'не указано'} до {vacancy['salary_to'] or 'не указано'} {vacancy['currency'] or ''}"

            text += f"\n<b>НАЗВАНИЕ:</b> <a href='{html.escape(vacancy['vacancy_url'])}'>{html.escape(vacancy['name'])}</a>\n" \
                    f"<b>РАБОТОДАТЕЛЬ:</b> {html.escape(vacancy['employer_name'])}\n" \
                    f"<b>ГОРОД:</b> {html.escape(vacancy['city'])}\n" \
                    f"<b>ЗАРПЛАТА:</b> {html.escape(salary)}\n" \
                    f"<b>ТРЕБОВАНИЯ:</b> {html.escape(vacancy['requirement'])}\n" \
                    f"<b>ОБЯЗАННОСТИ:</b> {html.escape(vacancy['responsibility'])}\n" \
                    f"<b>ДАТА ПУБЛИКАЦИИ:</b> {formatted_date}\n"
            text = text_change(text)

        return text

    except psycopg2.Error as error:
        print(f"Error inserting data: {error}")
        logger.error(f'Error inserting data: {error}')
        return None


def get_top_salary_vacancies(tgid):
    try:
        query = """
                SELECT * FROM vacancies 
                WHERE tgid = %s 
                ORDER BY COALESCE(salary_to, salary_from) DESC NULLS LAST 
                LIMIT 5
                """

        cursor.execute(query, (tgid,))
        vacancies = cursor.fetchall()

        text = "<b>Топ 5 вакансий с самой высокой зарплатой:</b>\n"

        for vacancy in vacancies:
            if isinstance(vacancy[11], datetime):
                formatted_date = vacancy[11].strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(vacancy[11], str):
                formatted_date = datetime.fromisoformat(vacancy[11]).strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = "Дата не указана"

            salary = "Не указана"
            if vacancy[5] or vacancy[6]:
                salary = f"от {vacancy[5] or 'не указано'} до {vacancy[6] or 'не указано'} {vacancy[7] or ''}"

            text += f"\n<b>НАЗВАНИЕ:</b> <a href='{html.escape(str(vacancy[2]))}'>{html.escape(str(vacancy[3]))}</a>\n" \
                    f"<b>РАБОТОДАТЕЛЬ:</b> {html.escape(str(vacancy[4]))}\n" \
                    f"<b>ГОРОД:</b> {html.escape(str(vacancy[8]))}\n" \
                    f"<b>ЗАРПЛАТА:</b> {html.escape(str(salary))}\n" \
                    f"<b>ТРЕБОВАНИЯ:</b> {html.escape(str(vacancy[9]))}\n" \
                    f"<b>ОБЯЗАННОСТИ:</b> {html.escape(str(vacancy[10]))}\n" \
                    f"<b>ДАТА ПУБЛИКАЦИИ:</b> {formatted_date}\n"

        text = text_change(text)
        return text

    except psycopg2.Error as error:
        print(f"Database error: {error}")
        logger.error(f'Database error top_sallary button: {error}')
        return "Произошла ошибка при получении данных из базы."


def send_all_vacancies(tgid):
    try:
        query = """
               SELECT * FROM vacancies 
               WHERE tgid = %s 
               """
        cursor.execute(query, (tgid,))
        vacancies = cursor.fetchall()
        text = "<b>Все найденные вакансии:</b>\n"
        messages = []

        for vacancy in vacancies:
            if isinstance(vacancy[11], datetime):
                formatted_date = vacancy[11].strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(vacancy[11], str):
                formatted_date = datetime.fromisoformat(vacancy[11]).strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = "Дата не указана"

            salary = "Не указана"

            if vacancy[5] or vacancy[6]:
                salary = f"от {vacancy[5] or 'не указано'} до {vacancy[6] or 'не указано'} {vacancy[7] or ''}"

            text += f"\n<b>НАЗВАНИЕ:</b> <a href='{html.escape(str(vacancy[2]))}'>{html.escape(str(vacancy[3]))}</a>\n" \
                    f"<b>РАБОТОДАТЕЛЬ:</b> {html.escape(str(vacancy[4]))}\n" \
                    f"<b>ГОРОД:</b> {html.escape(str(vacancy[8]))}\n" \
                    f"<b>ЗАРПЛАТА:</b> {html.escape(str(salary))}\n" \
                    f"<b>ДАТА ПУБЛИКАЦИИ:</b> {formatted_date}\n"
            text = text_change(text)

            if len(text) > MAX_LENGTH:
                messages.append(text)
                text = ""

        messages.append(text)
        return messages

    except psycopg2.Error as error:
        print(f"Database error: {error}")
        logger.error(f'Database error all_vacancies button: {error}')
        return ["Произошла ошибка при получении данных из базы."]


def top_5_vacancies_by_published_date(tgid):
    try:
        query = """
           SELECT * FROM vacancies 
           WHERE tgid = %s 
           ORDER BY published_at DESC
           LIMIT 5
           """

        cursor.execute(query, (tgid,))
        vacancies = cursor.fetchall()
        text = "<b>Топ 5 вакансий по дате публикации:</b>\n"

        for vacancy in vacancies:
            if isinstance(vacancy[11], datetime):
                formatted_date = vacancy[11].strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(vacancy[11], str):
                formatted_date = datetime.fromisoformat(vacancy[11]).strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = "Дата не указана"

            salary = "Не указана"
            if vacancy[5] or vacancy[6]:
                salary = f"от {vacancy[5] or 'не указано'} до {vacancy[6] or 'не указано'} {vacancy[7] or ''}"  # currency

            text += f"\n<b>НАЗВАНИЕ:</b> <a href='{html.escape(str(vacancy[2]))}'>{html.escape(str(vacancy[3]))}</a>\n" \
                    f"<b>РАБОТОДАТЕЛЬ:</b> {html.escape(str(vacancy[4]))}\n" \
                    f"<b>ГОРОД:</b> {html.escape(str(vacancy[8]))}\n" \
                    f"<b>ЗАРПЛАТА:</b> {html.escape(str(salary))}\n" \
                    f"<b>ТРЕБОВАНИЯ:</b> {html.escape(str(vacancy[9]))}\n" \
                    f"<b>ОБЯЗАННОСТИ:</b> {html.escape(str(vacancy[10]))}\n" \
                    f"<b>ДАТА ПУБЛИКАЦИИ:</b> {formatted_date}\n"

        text = text_change(text)
        return text

    except psycopg2.Error as error:
        print(f"Database error: {error}")
        logger.error(f'Database error top_date button: {error}')
        return "Произошла ошибка при получении данных из базы."


def delete_user(tgid):
    cursor.execute("DELETE FROM vacancies WHERE tgid = %s", (tgid,))
    conn.commit()


def get_city_id(city):
    url = 'https://api.hh.ru/suggests/areas'
    params = {'text': city}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        areas = response.json().get('items', [])
        if areas:
            return areas[0].get('id')
    return None


def dm(message):
    try:
        bot.delete_message(message.chat.id, message.message_id)
        if message.chat.id in last_message_id:
            messages_to_delete = last_message_id[message.chat.id]
            bot.delete_messages(message.chat.id, messages_to_delete)
            last_message_id[message.chat.id] = []
    except Exception as error:
        # Не критичная ошибка
        pass


def replace_callbacks_with_button_names(text):
    replacements = {
        'fullDay': 'Полный рабочий день',
        'shift': 'Сменный график',
        'flexible': 'Гибкий график',
        'remote': 'Удаленная работа',
        'noExperience': 'Нет опыта',
        'between1And3': 'От 1 года до 3 лет',
        'between3And6': 'От 3 до 6 лет',
        'moreThan6': 'Более 6 лет'
    }

    for callback, button_name in replacements.items():
        text = text.replace(callback, button_name)
    return text


@bot.message_handler(commands=['start'])
def start(message):
    if message.chat.id not in last_message_id:
        last_message_id[message.chat.id] = []
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("Начать", callback_data='start'),
    )
    message = bot.send_message(
        message.chat.id,
        "👋 Добро пожаловать в наш телеграм-бот по поиску и анализу вакансий!"
        "\n🔍 Здесь вы сможете найти идеальную работу, заполнив всего несколько полей:"
        "\n- Название вакансии"
        "\n- Город"
        "\n- Желаемая заработная плата"
        "\n- Опыт работы"
        "\n- Тип занятости"
        "\n\n🚀 Не упустите возможность сделать следующий шаг в своей карьере!"
        "\n🌟 Готовы начать? Давайте найдем вашу идеальную вакансию вместе!",
        reply_markup=markup
    )
    last_message_id[message.chat.id].append(message.id)


def get_vacancy_title(message):
    vacancy_instances[message.chat.id].title = message.text
    dm(message)
    message = bot.send_message(message.chat.id, "Пожалуйста, укажите город, в котором вы ищете работу.")
    last_message_id[message.chat.id].append(message.id)
    bot.register_next_step_handler(message, get_vacancy_city)


def get_vacancy_city(message):
    city = message.text
    if get_city_id(city) is not None:
        dm(message)
        vacancy_instances[message.chat.id].city = city
        message = bot.send_message(message.chat.id, "Пожалуйста, укажите желаемую заработную плату в RUR.")
        last_message_id[message.chat.id].append(message.id)
        bot.register_next_step_handler(message, get_vacancy_salary)
    else:
        dm(message)
        message = bot.send_message(message.chat.id, "Попробуйте ввести название города еще раз. "
                                                    "Если не получится, возможно, его нет в нашей базе данных.")
        last_message_id[message.chat.id].append(message.id)
        bot.register_next_step_handler(message, get_vacancy_city)


def get_vacancy_salary(message):
    salary = message.text
    if salary.isdigit() and int(salary) > 0:
        dm(message)
        vacancy_instances[message.chat.id].salary = salary
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Нет опыта", callback_data='0exp'),
            InlineKeyboardButton("От 1 года до 3 лет", callback_data='1exp3'),
            InlineKeyboardButton("От 3 до 6 лет", callback_data='3exp6'),
            InlineKeyboardButton("Более 6 лет", callback_data='6exp'),
        )
        message = bot.send_message(message.chat.id, "Пожалуйста, укажите ваш опыт работы.", reply_markup=markup)
        last_message_id[message.chat.id].append(message.id)
    else:
        dm(message)
        message = bot.send_message(message.chat.id, "Введите корректное значение заработной платы")
        last_message_id[message.chat.id].append(message.id)
        bot.register_next_step_handler(message, get_vacancy_salary)


def get_vacancy_schedule(message):
    dm(message)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Полный рабочий день", callback_data='fullDay'),
        InlineKeyboardButton("Сменный график", callback_data='shift'),
        InlineKeyboardButton("Гибкий график", callback_data='flexible'),
        InlineKeyboardButton("Удаленная работа", callback_data='remote'),
    )
    message = bot.send_message(message.chat.id, "Пожалуйста, укажите тип занятости.", reply_markup=markup)
    last_message_id[message.chat.id].append(message.id)


def get_vacancy_change(message):
    dm(message)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Все правильно", callback_data='correct'),
        InlineKeyboardButton("Изменить", callback_data='change'),
    )
    message = bot.send_message(message.chat.id,
                               replace_callbacks_with_button_names(vacancy_instances[message.chat.id].send_aform()),
                               reply_markup=markup)
    last_message_id[message.chat.id].append(message.id)


def edit_title(message):
    vacancy_instances[message.chat.id].title = message.text
    dm(message)
    get_vacancy_change(message)


def edit_city(message):
    city = message.text
    if get_city_id(city) is not None:
        dm(message)
        vacancy_instances[message.chat.id].city = city
        get_vacancy_change(message)
    else:
        dm(message)
        message = bot.send_message(message.chat.id, "Попробуйте ввести название города еще раз. "
                                                    "Если не получится, возможно, его нет в нашей базе данных.")
        last_message_id[message.chat.id].append(message.id)
        bot.register_next_step_handler(message, edit_city)


def edit_salary(message):
    salary = message.text
    if salary.isdigit() and int(salary) > 0:
        dm(message)
        vacancy_instances[message.chat.id].salary = salary
        get_vacancy_change(message)
    else:
        dm(message)
        message = bot.send_message(message.chat.id, "Введите корректное значение заработной платы")
        last_message_id[message.chat.id].append(message.id)
        bot.register_next_step_handler(message, edit_salary)


@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    chat_id = call.message.chat.id
    if call.data == "start":
        dm(call.message)
        if chat_id not in vacancy_instances:
            vacancy_instances[chat_id] = Vacancy()
        message = bot.send_message(chat_id, "Для поиска вакансии, пожалуйста, укажите ее название")
        last_message_id[chat_id].append(message.id)
        bot.register_next_step_handler(call.message, get_vacancy_title)
        delete_user(chat_id)
    elif call.data == "change":
        edit_menu_keyboard = InlineKeyboardMarkup(row_width=2)
        edit_menu_keyboard.add(
            InlineKeyboardButton("Изменить название вакансии", callback_data='edit_title'),
            InlineKeyboardButton("Изменить город", callback_data='edit_city'),
            InlineKeyboardButton("Изменить опыт работы", callback_data='edit_experience'),
            InlineKeyboardButton("Изменить тип занятости", callback_data='edit_schedule'),
            InlineKeyboardButton("Изменить желаемую зарплату", callback_data='edit_salary')
        )
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=edit_menu_keyboard)
    experience_mapping = {
        "0exp": "noExperience",
        "1exp3": "between1And3",
        "3exp6": "between3And6",
        "6exp": "moreThan6",
        "0expe": "noExperience",
        "1exp3e": "between1And3",
        "3exp6e": "between3And6",
        "6expe": "moreThan6"
    }
    schedule_mapping = {
        "fullDay": "fullDay",
        "shift": "shift",
        "flexible": "flexible",
        "remote": "remote"
    }
    if call.data in experience_mapping:
        vacancy_instances[chat_id].experience = experience_mapping[call.data]
        if call.data.endswith('e'):
            get_vacancy_change(call.message)
        else:
            get_vacancy_schedule(call.message)

    elif call.data in schedule_mapping:
        vacancy_instances[chat_id].schedule = schedule_mapping[call.data]
        get_vacancy_change(call.message)
    edit_actions = {
        "edit_title": ("Пожалуйста, укажите название вакансии.", edit_title),
        "edit_city": ("Пожалуйста, укажите город, в котором вы ищете работу.", edit_city),
        "edit_salary": ("Пожалуйста, укажите желаемую зарплату.", edit_salary)
    }
    if call.data in edit_actions:
        dm(call.message)
        message = bot.send_message(chat_id, edit_actions[call.data][0])
        last_message_id[chat_id].append(message.id)
        bot.register_next_step_handler(call.message, edit_actions[call.data][1])
    elif call.data == "edit_experience":
        dm(call.message)
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Нет опыта", callback_data='0expe'),
            InlineKeyboardButton("От 1 года до 3 лет", callback_data='1exp3e'),
            InlineKeyboardButton("От 3 до 6 лет", callback_data='3exp6e'),
            InlineKeyboardButton("Более 6 лет", callback_data='6expe'),
        )
        message = bot.send_message(chat_id, "Пожалуйста, укажите ваш опыт работы.", reply_markup=markup)
        last_message_id[chat_id].append(message.id)
    elif call.data == "edit_schedule":
        dm(call.message)
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Полный рабочий день", callback_data='fullDay'),
            InlineKeyboardButton("Сменный график", callback_data='shift'),
            InlineKeyboardButton("Гибкий график", callback_data='flexible'),
            InlineKeyboardButton("Удаленная работа", callback_data='remote'),
        )
        message = bot.send_message(chat_id, "Пожалуйста, укажите тип занятости.", reply_markup=markup)
        last_message_id[chat_id].append(message.id)
    elif call.data == "correct":
        url = 'https://api.hh.ru/vacancies'
        headers = {'User-Agent': 'YourAppName/1.0'}
        params = {
            'text': vacancy_instances[chat_id].title,
            'salary': vacancy_instances[chat_id].salary,
            'experience': vacancy_instances[chat_id].experience,
            'area': get_city_id(vacancy_instances[chat_id].city),
            'schedule': vacancy_instances[chat_id].schedule,
            'per_page': 100
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            vacancies = response.json().get('items', [])
            text = parsing_table(vacancies, chat_id)
            if text:
                dm(call.message)
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(
                    InlineKeyboardButton("Вывести 5 вакансий с самой большой ЗП", callback_data='topsalary'),
                    InlineKeyboardButton("Вывести 5 самых новых вакансий", callback_data='topdate'),
                    InlineKeyboardButton("Вывести все доступные вакансии", callback_data='all_v'),
                    InlineKeyboardButton("Создать новый запрос (старый будет удален)", callback_data='start'),
                )
                message = bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
                last_message_id[chat_id].append(message.id)
            else:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(
                    InlineKeyboardButton("Создать новый запрос", callback_data='start'),
                )
                message = bot.send_message(chat_id, "По вашему запросу ничего не найдено, попробуйте еще раз",
                                           reply_markup=markup)
                last_message_id[chat_id].append(message.id)

        else:
            dm(call.message)
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("Создать новый запрос", callback_data='start'),
            )
            message = bot.send_message(chat_id, "Ошибка подключения к сайту.\n"
                                                "Подождите и попробуйте создать запрос заново",
                                       reply_markup=markup)
            last_message_id[chat_id].append(message.id)
    elif call.data == 'topsalary':
        dm(call.message)
        text = get_top_salary_vacancies(chat_id)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("Вывести 5 самых новых вакансий", callback_data='topdate'),
            InlineKeyboardButton("Вывести все доступные вакансии", callback_data='all_v'),
            InlineKeyboardButton("Создать новый запрос (старый будет удален)", callback_data='start'),
        )
        message = bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
        last_message_id[chat_id].append(message.id)
    elif call.data == 'topdate':
        dm(call.message)
        text = top_5_vacancies_by_published_date(chat_id)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("Вывести 5 вакансий с самой большой ЗП", callback_data='topsalary'),
            InlineKeyboardButton("Вывести все доступные вакансии", callback_data='all_v'),
            InlineKeyboardButton("Создать новый запрос (старый будет удален)", callback_data='start'),
        )
        message = bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
        last_message_id[chat_id].append(message.id)
    elif call.data == 'all_v':
        dm(call.message)
        messages = send_all_vacancies(chat_id)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("Вывести 5 самых новых вакансий", callback_data='topdate'),
            InlineKeyboardButton("Вывести 5 вакансий с самой большой ЗП", callback_data='topsalary'),
            InlineKeyboardButton("Создать новый запрос (старый будет удален)", callback_data='start'),
        )
        for i in range(len(messages) - 1):
            message = bot.send_message(chat_id, messages[i], parse_mode='HTML')
            last_message_id[chat_id].append(message.id)

        message = bot.send_message(chat_id, messages[-1], parse_mode='HTML', reply_markup=markup)
        last_message_id[chat_id].append(message.id)


if __name__ == '__main__':
    create_database()
    create_table()
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception as e:
            print('Error occurred:', str(e))
            logger.error(f'Error occurred: {e}')
            time.sleep(15)
