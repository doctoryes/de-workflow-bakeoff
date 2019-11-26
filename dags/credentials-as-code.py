"""
A simple one-task DAG to programmatically and idempotently set system-wide credentials.

After running this DAG, check the results by going to Admin -> Connections.
"""
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2019, 11, 26),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_credentials',
    max_active_runs=1,
    schedule_interval=timedelta(minutes=15),
    default_args=default_args,
    catchup=False
)

def create_conn(conn_id, password, login, host):
    """
    Idempotently create or update an airflow "Connection".
    """
    session = settings.Session()
    matching_conns = session.query(Connection).filter_by(conn_id=conn_id)
    if matching_conns.count() > 1:
        raise RuntimeError('Multiple connections matching conn_id="{}".  Delete one from the UI!'.format(conn_id))
    elif matching_conns.count() == 1:
        print('connection {} found!'.format(conn_id))
        existing_conn = matching_conns.first()
        existing_conn.login = login
        existing_conn.host = host
        existing_conn.set_password(password)
    else:
        print('connection {} not found!'.format(conn_id))
        new_conn = Connection(conn_id=conn_id, login=login, host=host)
        new_conn.set_password(password)
        session.add(new_conn)
    session.commit()


def create_all_connections():
    create_conn('test_user_1', 'password', login='test_user_1', host='example.com')
    create_conn('test_user_2', 'password', login='test_user_2', host='example.com')
    create_conn('test_user_3', 'password', login='test_user_3', host='example.com')


run_this = PythonOperator(
    task_id='create_all_connections',
    python_callable=create_all_connections,
    dag=dag,
)
