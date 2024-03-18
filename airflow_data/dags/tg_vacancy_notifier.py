from airflow.decorators import dag, task
from datetime import datetime, timedelta
from config_file import DWH_HOOK, TG_HOOK
import time


default_args = {
    'depends_on_past': False,
    "start_date": datetime(2024, 3, 13),
    "catchup": False,
    "tags": "hh_parsing"
}

@dag(default_args=default_args, description='Telegram bot for daily vacancies', schedule_interval=None)
def tg_vacancy_notifier():

    @task()
    def fetch_vacancies() -> list:
        """Fetch vacancies from DWH within today"""

        cur_date = datetime.now().date()
        sql = f"""
        SELECT v.name, v.employer, v.experience, v.format, v.link, coalesce(STRING_AGG(s.skill, ', '), 'Не указаны') as skills
        FROM vacancies v
        LEFT JOIN skills s on v.id = s.id
        WHERE DATE(v.created_at) = '{cur_date}'
        GROUP BY v.name, v.employer, v.experience, v.format, v.link;
        """
        vacancies = DWH_HOOK.get_records(sql)
        return vacancies

    @task()
    def send_messages(vacancies: list) -> None:
        """Send message with actual vacancies"""

        cur_date = datetime.now().strftime('%d.%m.%Y')
        if not vacancies:
            TG_HOOK.send_message({
                'text': f"Добрый день. Сегодня {cur_date} новых вакансий нет.",
                'parse_mode': 'HTML'
            })
            return
        
        intro_message = f"Добрый день. Новые вакансии на <b>{cur_date}</b>:"
        TG_HOOK.send_message({
            'text': intro_message,
            'parse_mode': 'HTML'
        })
        time.sleep(2)  # Задержка перед отправкой списка вакансий
        
        vac_per_message = 10
        msg_cnt = 0
        for i in range(0, len(vacancies), vac_per_message):
            message_parts = []
            for vacancy in vacancies[i:i+vac_per_message]:
                name, employer, experience, work_format, link, skills = vacancy
                message_parts.append(f"<b>{name}</b> в компанию <b>{employer}</b>\n<b>Опыт:</b> {experience}\n<b>Формат работы:</b> {work_format}\n<b>Навыки:</b> {skills}\n<b>Ссылка:</b> {link}\n")
            
            message = "\n".join(message_parts)
            TG_HOOK.send_message({
            'text': message,
            'parse_mode': 'HTML'
            })
            time.sleep(3)
            msg_cnt += 1
        print(f"Отправлено сообщений с вакансиями - {msg_cnt}")

    vacancies = fetch_vacancies()
    send_messages(vacancies)

dag = tg_vacancy_notifier()
