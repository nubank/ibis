# Copyright 2016 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy as sa

from ibis.client import Database
from .compiler import RedshiftDialect
import ibis.expr.types as ir
import ibis.sql.alchemy as alch


class RedshiftTable(alch.AlchemyTable):
    pass


class RedshiftDatabase(Database):
    pass


class RedshiftClient(alch.AlchemyClient):

    """
    The Ibis Redshift client class
    """

    dialect = RedshiftDialect
    database_class = RedshiftDatabase

    def __init__(self, host=None, user=None, password=None, port=None,
                 database=None, url=None, driver=None, schema=None):
        if url is None:
            if user is not None:
                if password is None:
                    userpass = user
                else:
                    userpass = '{0}:{1}'.format(user, password)

                address = '{0}@{1}'.format(userpass, host)
            else:
                address = host

            if port is not None:
                address = '{0}:{1}'.format(address, port)

            if database is not None:
                address = '{0}/{1}'.format(address, database)

            if driver is not None and driver != 'psycopg2':
                raise NotImplementedError(driver)

            url = 'postgresql://{0}'.format(address)

        if schema is None:
            schema = 'public'

        url = sa.engine.url.make_url(url)
        self.name = url.database
        self.schema = schema
        self.con = sa.create_engine(url)
        self.meta = sa.MetaData(bind=self.con)

    @property
    def current_database(self):
        return self.name

    @property
    def current_schema(self):
        return self.schema

    def list_databases(self):
        # http://dba.stackexchange.com/a/1304/58517
        return [
            row.datname for row in self.con.execute(
                'SELECT datname FROM pg_database WHERE NOT datistemplate'
            )
        ]

    def list_tables(self, like=None, database=None):
        """
        List tables in the current (or indicated) schema.

        Parameters
        ----------
        like : string, default None
          Checks for this string contained in name
        database : string, default None
          If not passed, uses the current/default schema

        Returns
        -------
        tables : list of strings
        """
        if database is None:
            database = self.schema
        names = self.con.table_names(schema=database)
        if like is not None:
            names = [x for x in names if like in x]
        return names

    def set_database(self):
        raise NotImplementedError

    @property
    def client(self):
        return self

    def table(self, name, database=None):
        """
        Create a table expression that references a particular table in the
        PostgreSQL database

        Parameters
        ----------
        name : string

        Returns
        -------
        table : TableExpr
        """
        if database is None:
            database = self.current_schema
        alch_table = self._get_sqla_table(name, schema=database)
        node = RedshiftTable(alch_table, self)
        return self._table_expr_klass(node)

    def _get_sqla_table(self, name, schema=None):
        if schema is None:
            schema = self.current_schema
        return sa.Table(name, self.meta, schema=schema, autoload=True)

    def drop_table(self):
        pass

    def create_table(self, name, expr=None, schema=None, database=None):
        pass

    @property
    def _table_expr_klass(self):
        return ir.TableExpr
