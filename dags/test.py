from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.context import Context
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults

# ------------------------------------------------------------------
# DEFAULT ARGS (Punto 1)
# ------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1900, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# ------------------------------------------------------------------
# DAG definition (ejecución diaria a las 03:00 UTC)
# ------------------------------------------------------------------
with DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval='0 3 * * *',  
    catchup=False,
    tags=['prueba_tecnica']
) as dag:

    # ------------------------------------------------------------------
    # Punto 2: tareas start y end
    # ------------------------------------------------------------------
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # ------------------------------------------------------------------
    # Punto 3: lista de tareas dummy task_n
    # Cada tarea par depende de todas las impares
    # ------------------------------------------------------------------
    N = 6  # se puede cambiar

    tasks = {}
    for i in range(1, N + 1):
        tasks[i] = DummyOperator(task_id=f'task_{i}')

    # dependencias: pares dependen de impares
    odd_tasks = [tasks[i] for i in range(1, N + 1) if i % 2 != 0]
    for i in range(1, N + 1):
        if i % 2 == 0:
            for odd in odd_tasks:
                odd >> tasks[i]

    # start -> todos los tasks -> end
    start >> list(tasks.values()) >> end

    # ------------------------------------------------------------------
    # Punto 4: Operador personalizado TimeDiff
    # ------------------------------------------------------------------
    class TimeDiff(BaseOperator):
        """
        Operador que recibe una fecha (diff_date)
        y muestra la diferencia con la fecha actual.
        """

        @apply_defaults
        def __init__(self, diff_date: datetime, **kwargs):
            super().__init__(**kwargs)
            self.diff_date = diff_date

        def execute(self, context: Context):
            now = timezone.utcnow()
            diff = now - self.diff_date
            self.log.info(
                f"Diferencia entre {now} y {self.diff_date}: {diff}"
            )
            return diff

    time_diff_task = TimeDiff(
        task_id='time_diff_task',
        diff_date=datetime(2020, 1, 1)
    )

    start >> time_diff_task >> end


# ------------------------------------------------------------------
# ¿Qué es un Hook? ¿En qué se diferencia de una conexión?
#
# En Airflow:
# - Una CONEXIÓN es solo la configuración (host, usuario, password,
#   token, schema, etc.) almacenada en Airflow y referenciada por conn_id.
#
# - Un HOOK es una clase Python que usa una conexión para interactuar
#   con un sistema externo (por ejemplo, PostgresHook, BigQueryHook).
#
# Resumen:
#   Conexión = configuración
#   Hook = lógica + uso de la conexión
# ------------------------------------------------------------------