from airflow.models import Variable
import pendulum
from airflow.decorators import dag, task
import boto3
import logging
from project.loader import loader


log = logging.getLogger(__name__)

def fetch_s3_file(key: str):
    AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file( Bucket='sprint6', Key=key, Filename='/data/'+key) 

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint6', 'example', 'get_files'],
    is_paused_upon_creation=False
)
def project():
    files = ["group_log.csv"]
    link_loader = loader(log=log)

    @task(task_id="get_csv")
    def get_csv(v_files):
        for file in v_files:
            fetch_s3_file(file)
    
    @task(task_id="load_csv")
    def load_csv():
        link_loader.load_csv(columns=['group_id', 'user_id', 'user_id_from', 'event', 'datetime'], 
                            dataset_path='/data/group_log.csv',
                            schema='FARRUHRUSYANDEXRU__STAGING', 
                            table='group_log')
        
    @task(task_id="load_link")
    def load_link():
        link_loader.load_link()

    @task(task_id="load_satellite")
    def load_satellite():
        link_loader.load_satellite()

    # init    
    get_csv=get_csv(files)
    load_csv=load_csv()
    load_link=load_link()
    load_satellite=load_satellite()

    get_csv >> load_csv >> load_link >> load_satellite

project = project()
project

log.info("Finished")

