import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)

class PostgreSQLCountRows(BaseOperator):
    """ operator to count table rows """
    @apply_defaults
    def __init__(self, table_name, connection_id,
                 *args, **kwargs):
        """
        :param table_name: table name
        :param connection_id: sql connection id
        """
        self.table_name = table_name
        self.connection_id = connection_id
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.connection_id)
        result = hook.get_first('select count(1) from {}'.format(self.table_name))
        return result[0]

class PostgreSQLCustomOperatorsPlugin(AirflowPlugin):
    name = 'postgres_custom'
    operators = [PostgreSQLCountRows]
