import random
import time
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr


def pick_medal_def(ti):
    return random.choices(['Bronze', 'Silver', 'Gold'])[0]


def pick_medal_task_def(ti):
    medal_type = ti.xcom_pull(task_ids='pick_medal')

    next_task = None

    match medal_type:
        case 'Bronze':
            next_task = 'calc_Bronze'
        case 'Silver':
            next_task = 'calc_Silver'
        case 'Gold':
            next_task = 'calc_Gold'
    return next_task


def generate_delay_def():
    time.sleep(5)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_oleksiik"

with DAG(
        'ok_hw_7_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["oleksiik_hw"]
) as dag:
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS oleksiik.medals_hw (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `medal_type` VARCHAR(30),
        `count` INT,
        `created_at` DATETIME
        );
        """
    )

    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        trigger_rule=tr.ONE_SUCCESS,
        python_callable=pick_medal_def,
        provide_context=True,
        dag=dag
    )

    pick_medal_branch_task = BranchPythonOperator(
        task_id='pick_medal_task',
        trigger_rule=tr.ONE_SUCCESS,
        python_callable=pick_medal_task_def,
        provide_context=True,
        dag=dag
    )

    calc_bronze_task = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO oleksiik.medals_hw
        (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*) as count, NOW() as created_at
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Bronze'
        """
    )

    calc_silver_task = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO oleksiik.medals_hw
        (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*) as count, NOW() as created_at
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Silver'
        """
    )

    calc_gold_task = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO oleksiik.medals_hw
        (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*) as count, NOW() as created_at
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Gold'
        """
    )

    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        trigger_rule=tr.ONE_SUCCESS,
        python_callable=generate_delay_def,
    )

    check_for_correctness_task = SqlSensor(
        task_id='check_for_correctness',
        trigger_rule=tr.ONE_SUCCESS,
        conn_id=connection_name,
        sql="""SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) < 30 FROM oleksiik.medals_hw
                ORDER BY created_at DESC LIMIT 1
               ;""",
        mode='poke',
        poke_interval=5,
        timeout=6,
    )

    create_table >> pick_medal_task >> pick_medal_branch_task
    pick_medal_branch_task >> [calc_bronze_task, calc_silver_task, calc_gold_task]
    calc_bronze_task >> generate_delay_task
    calc_silver_task >> generate_delay_task
    calc_gold_task >> generate_delay_task
    generate_delay_task >> check_for_correctness_task
