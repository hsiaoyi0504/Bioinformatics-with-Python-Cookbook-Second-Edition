""" Task creator
"""

from datetime import datetime, timedelta
from os.path import isfile

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.sensors.file_sensor import FileSensor


dag_args = {
    'owner': 'airflow',
    'description': 'Bioinformatics with Python Cookbook pipeline',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 18),
    'email': ['your@email.here'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

ftp_directory = '/hapmap/genotypes/hapmap3/plink_format/draft_2/'
ftp_files = {
    'hapmap3_r2_b36_fwd.consensus.qc.poly.map.bz2': 'hapmap.map.bz2',
    'hapmap3_r2_b36_fwd.consensus.qc.poly.ped.bz2': 'hapmap.ped.bz2',
    'relationships_w_pops_121708.txt': 'relationships.txt',
}


def download_files(ds, **kwargs):
    with FTPHook('ftp_ncbi') as conn:
        for ftp_name, local_name in ftp_files.items():
            if not isfile(local_name):
                conn.retrieve_file(ftp_directory + ftp_name, local_name)
    #NEED FSHook!!!


def subsample_10p(ds, **kwargs):
    return

dag = DAG('bioinf', default_args=dag_args, schedule_interval=None)

t_dowload_files = PythonOperator(
    task_id='download_files',
    provide_context=True,
    python_callable=download_files,
    dag=dag)

#s_rel_sensor = FileSensor(
#    task_id='download_sensor',
#    fs_conn_id='fs_bioinf',
#    filepath='relationships.txt',
#    dag=dag)

#t_subsample_10p = PythonOperator(
#    task_id='subsample_10p',
#    provide_context=True,
#    python_callable=subsample_10p,
#    dag=dag)


#s_rel_sensor.set_upstream(t_dowload_files) #!!!
#t_subsample_10p.set_upstream(s_rel_sensor)
