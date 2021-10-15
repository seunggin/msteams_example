import base64
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import DAG
from ms_teams_webhook_operator import MSTeamsWebhookOperator
from kubernetes import client, config

config.load_incluster_config()
v1 = client.CoreV1Api()

secret = v1.read_namespaced_secret("minio-secret", namespace="default").data
MINIO_ACCESS_KEY = str(base64.b64decode(secret["MINIO_ACCESS_KEY"]), "utf-8")
MINIO_SECRET_KEY = str(base64.b64decode(secret["MINIO_SECRET_KEY"]), "utf-8")

secret = v1.read_namespaced_secret("mongodb-secret", namespace="default").data
MONGODB_USERNAME = str(base64.b64decode(secret["MONGODB_USERNAME"]), "utf-8")
MONGODB_PASSWORD = str(base64.b64decode(secret["MONGODB_PASSWORD"]), "utf-8")

MINIO_HOSTNAME = "http://minio.minio.svc.cluster.local"

MONGODB_HOSTNAME = "mongodb-mongos.mongodb.svc.cluster.local"
MONGODB_PORT = "27017"
MSTEAMSWBEHOOKURL = "msteams-webhook-url"


def on_failure(context):

    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)

    logs_url = "https://myairflow/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
        dag_id, task_id, context['ts'])

    teams_notification = MSTeamsWebhookOperator(
        task_id="msteams_notify_failure", trigger_rule="all_done",
        message="`{}` has failed on task: `{}`".format(dag_id, task_id),
        button_text="View log", button_url=logs_url,
        theme_color="FF0000", http_conn_id=MSTEAMSWBEHOOKURL)
    teams_notification.execute(context)


def on_success(context):
    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)

    logs_url = "https://myairflow/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
        dag_id, task_id, context['ts'])

    teams_notification = MSTeamsWebhookOperator(
        task_id="msteams_notify_success", trigger_rule="all_done",
        message="`{}` has success on task: `{}`".format(dag_id, task_id),
        button_text="View log", button_url=logs_url,
        theme_color="008000", http_conn_id=MSTEAMSWBEHOOKURL)
    teams_notification.execute(context)


dag = DAG(
    dag_id="backup-mongodb",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *",
    on_failure_callback=on_failure,
    on_success_callback=on_success,
)

t0 = BashOperator(
    task_id="backup-mongodb",
    bash_command="mc config host add minio {} {} {} && mongodump -h {}:{} -u {} -p {} --gzip --archive | mc pipe minio/mongodb/backups/clever-{}.gz".format(
        MINIO_HOSTNAME,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        MONGODB_HOSTNAME,
        MONGODB_PORT,
        MONGODB_USERNAME,
        MONGODB_PASSWORD,
        datetime.today().strftime("%Y%m%d"),
    ),
    dag=dag,
)

t0
