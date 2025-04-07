from datetime import timedelta
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    # 'start_date': airflow.utils.dates.days_ago(1),
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['kkyalamanchili@globe.life'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True
}

dag = DAG(
    'cfo_monthlyrun_manual',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=5),
    schedule_interval=None, #
    start_date=days_ago(1)
)

bash = BashOperator(
    task_id='bash',
    bash_command='echo cfo vmf running',
    dag=dag
    )
	
def delete_files():
  s3 = boto3.resource('s3')
  s3_bucket = s3.Bucket('gl-cdm-dev-actuarial')
  s3_bucket.objects.filter(Prefix="vmf/cfo/poly").delete()
  # s3_bucket.objects.all().delete()


delete_s3cfopolyfolder = PythonOperator(
  task_id='delete_s3cfopolyfolder',
  python_callable=delete_files,
  dag=dag
  )
 
run_cfoinsertdataintocfobase = AWSAthenaOperator(
    task_id="run_cfoinsertdataintocfobase",
    query='sql/vmf/cfo/CFO_to_Poly_Base_Extract_QueryInsert.sql',
    output_location=f's3://gl-cdm-dev-actuarial/vmf/cfo/inserttableslogs',
    database='"AwsDataCatalog".actuarial_inputs'
        )

run_cfoinsertdataintocfocoverage = AWSAthenaOperator(
    task_id="run_cfoinsertdataintocfocoverage",
    query='sql/vmf/cfo/CFO_to_Poly_Cov_Extract_QueryInsert.sql',
    output_location=f's3://gl-cdm-dev-actuarial/vmf/cfo/inserttableslogs',
    database='"AwsDataCatalog".actuarial_inputs'
        )
run_cfoinsertdataintocfosbd0 = AWSAthenaOperator(
    task_id="run_cfoinsertdataintocfosbd0",
    query='sql/vmf/cfo/CFO_to_Poly_SB_D0_Extract_QueryInsert.sql',
    output_location=f's3://gl-cdm-dev-actuarial/vmf/cfo/inserttableslogs',
    database='"AwsDataCatalog".actuarial_inputs'
        )
run_cfoinsertdataintocfosbnond0 = AWSAthenaOperator(
    task_id="run_cfoinsertdataintocfosbnond0",
    query='sql/vmf/cfo/CFO_to_Poly_SB_Non_D0_Extract_QueryInsert.sql',
    output_location=f's3://gl-cdm-dev-actuarial/vmf/cfo/inserttableslogs',
    database='"AwsDataCatalog".actuarial_inputs'
        )	
		
runsp_cfovmfpolyfromathena = PostgresOperator(
        task_id="runsp_cfovmfpolyfromathena",
        postgres_conn_id="redshift_default",
        sql="CALL vmf_create.loadcfovmfpolydatafromathena();",
    )
				
bash >> delete_s3cfopolyfolder  >> run_cfoinsertdataintocfobase >> run_cfoinsertdataintocfocoverage >> run_cfoinsertdataintocfosbd0 >> run_cfoinsertdataintocfosbnond0 >> runsp_cfovmfpolyfromathena
