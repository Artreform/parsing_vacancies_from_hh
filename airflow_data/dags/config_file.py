from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from pathlib import Path
import json
import re


CONFIG = Variable.get('config', deserialize_json=True)
DWH_CONN_ID = 'dwh'
DWH_HOOK = PostgresHook(postgres_conn_id=DWH_CONN_ID)

TG_CONN_ID = 'vacancies_bot'
TG_HOOK = TelegramHook(telegram_conn_id=TG_CONN_ID)


def clean_cache(vacancies_list: list) -> None:
    """Cleaning cache folder from irrelevant vacancies"""

    actual_vacancies = [vacancy['id'] for vacancy in vacancies_list]
    cache_dir = Path(CONFIG["paths"]['cache_dir'])
    cache_dir.mkdir(parents=True, exist_ok=True)
    vac_del_cnt = 0
    for file_path in cache_dir.iterdir():
        if file_path.is_file():
            vacancy_id = file_path.stem  # Получаем имя файла (без расширения)
            if vacancy_id not in actual_vacancies:
                file_path.unlink()  # Удаляем файл
                vac_del_cnt += 1
    print(f"Удалено {vac_del_cnt} файлов из кэша - (вакансии неактуальны)")

def cache_vac(vacancy_id: str, vacancy_detail: dict) -> None:
    """Saving vacancies to the cache folder"""

    cache_dir = Path(CONFIG["paths"]['cache_dir'])
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{vacancy_id}.json"
    cache_file.write_text(json.dumps(vacancy_detail, ensure_ascii=False, indent=4))

def load_from_cache(vacancy_id: str) -> None:
    """Load vacancy details from cache"""

    cache_dir = Path(CONFIG["paths"]['cache_dir'])
    cache_file = cache_dir / f"{vacancy_id}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

def clean_tags(html_text: str) -> str:
    """Cleaning HTML tags"""

    pattern = re.compile('<.*?>')
    return re.sub(pattern, '', html_text)

def convert_gross(salary: float, is_gross: bool) -> float:
    """Converting salary from gross to net"""

    if is_gross:
        salary_net = salary * 0.87
    else:
        salary_net = salary
    return salary_net
