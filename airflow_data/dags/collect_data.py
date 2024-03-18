from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pathlib import Path
import requests
import json
import re
import pandas as pd
import time
from datetime import datetime
from config_file import CONFIG, DWH_HOOK, clean_cache, cache_vac, load_from_cache, clean_tags, convert_gross


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    "tags": "hh_parsing"
}


@dag(default_args = default_args, description = 'Pipeline to collect data from HH and load it to DWH', schedule_interval='0 15 * * *', catchup=False, max_active_runs=1)
def collect_data():

    @task()
    def create_tables():
        """Creating tables in Database"""

        for create_stmt in CONFIG["database"]["tables"].values():
            DWH_HOOK.run(create_stmt)

    @task()
    def truncate_tables(tables: list) -> None:
        """Cleaning tables before insert data"""

        for table in tables:
            sql = f"TRUNCATE TABLE {table};"
            try:
                DWH_HOOK.run(sql)
                print(f"Таблица {table} очищена")
            except Exception as e:
                print(f"Ошибка при очистке {table}: {e}")
    
    @task()
    def fetch_vacancies_list(url: str, search_params: dict) -> list[dict]:
        """Getting pages with vacancies"""

        vacancies_list = []
        pages_cnt = 0
        while True:
            try:
                response = requests.get(url, params=search_params)
                response.raise_for_status()
                data = response.json()
                vacancies_list.extend(data['items'])
                pages_cnt += 1
                print(f"Страница {search_params['page']} обработана. Обработано страниц: {pages_cnt}")
                if search_params["page"] >= data['pages'] - 1:
                    break
                search_params["page"] += 1
            except requests.exceptions.HTTPError as http_err:
                print(f"Возникла ошибка {http_err} при подключении к {url}") 
        print(f"Всего обработано {pages_cnt} страниц" )
        return vacancies_list
    

    @task()
    def fetch_vacancy_details(vacancies_list: list[dict]) -> dict:
        """Getting vacancy details from the list of vacancies"""

        vacancies_cnt = 0
        vac_api = 0
        vac_cache = 0

        vacancies = []
        salaries = []
        skills = []

        #очищаем папку кэш от неактуальных вакансий
        clean_cache(vacancies_list)
        
        for vacancy in vacancies_list:
            vacancy_id = vacancy['id']
            vacancy_detail = load_from_cache(vacancy_id)
            if vacancy_detail:
                vac_cache += 1
            else:
                try:
                    response = requests.get(vacancy['url'])
                    response.raise_for_status()
                    vacancy_detail = response.json()
                    cache_vac(vacancy_id, vacancy_detail)
                    vac_api += 1
                    time.sleep(2.0)
                except requests.exceptions.HTTPError as http_err:
                    print(f"Возникла ошибка {http_err}")
                    continue

            vacancies.append({
                'id': vacancy_id,
                'created_at': vacancy_detail['created_at'],
                'name': vacancy_detail['name'],
                'employer': vacancy_detail['employer']['name'],
                'format': vacancy_detail["schedule"]["name"],
                'experience': vacancy_detail["experience"]["name"],
                'description': clean_tags(vacancy_detail.get('description', '')),
                'link': vacancy_detail["alternate_url"],
            })

            if vacancy_detail.get('salary') and vacancy_detail['salary'].get('currency') == 'RUR':
                if vacancy_detail['salary'].get('from'):
                    salary_from = convert_gross(vacancy_detail['salary'].get('from'), vacancy_detail['salary'].get('gross'))
                else: 
                    salary_from = 0
                
                if vacancy_detail['salary'].get('to'):
                    salary_to = convert_gross(vacancy_detail['salary'].get('to'), vacancy_detail['salary'].get('gross'))
                else: 
                    salary_to = 0

                salaries.append({
                    'id': vacancy_id,
                    'salary_from': salary_from,
                    'salary_to': salary_to,
                })

            for skill in vacancy_detail.get('key_skills', []):
                skills.append({
                    'id': vacancy_id,
                    'skill': skill['name'],
                })
            vacancies_cnt += 1
        
        vacancies_details = {'vacancies': vacancies, 'salaries': salaries, 'skills': skills}

        print(f"Обработано {vacancies_cnt} вакансий, в том числе загружено из кэша - {vac_cache}, из api - {vac_api}")
        return vacancies_details
    
    @task
    def save_to_csv(data: dict) -> dict:
        """Export data with vacancies into csv files"""

        csv_dir = Path(CONFIG['paths']['csv_folder'])
        csv_dir.mkdir(parents=True, exist_ok=True)
        file_paths = {}
        for key, value in data.items():
            file_path = csv_dir / f"{key}.csv"
            df = pd.DataFrame(value)
            df.to_csv(file_path, index=False)
            file_paths[key] = str(file_path)
            print(f"В {key} сохранено {len(df)} записей")
        return file_paths

    @task
    def load_to_db(filepaths: dict) -> None:
        """Load data from CSV into DWH"""

        for table, file_path in filepaths.items():
            df = pd.read_csv(file_path)
            df.to_sql(table, DWH_HOOK.get_sqlalchemy_engine(), schema='public', if_exists='append', index=False)
            print(f"В БД таблицу {table} загружено {len(df)} записей")

        
    trigger_load_core_data = TriggerDagRunOperator(
        task_id='trigger_send_vacansies',
        trigger_dag_id='tg_vacancy_notifier',
        wait_for_completion=True,
        deferrable=True,
        retries=1
    )

    create_and_truncate = create_tables() >> truncate_tables(CONFIG["database"]["tables"].keys())
    vacansies_list = fetch_vacancies_list(CONFIG['api']['base_url'], CONFIG['api']['search_params'])
    vacancies_details = fetch_vacancy_details(vacansies_list)
    filepath_e = save_to_csv(vacancies_details)
    load_to_db = load_to_db(filepath_e)

    create_and_truncate >> vacansies_list >> vacancies_details >> filepath_e >> load_to_db >> trigger_load_core_data

dag = collect_data()
