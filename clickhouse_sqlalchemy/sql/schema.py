from sqlalchemy import Table as TableBase, inspection
from sqlalchemy.sql.base import (
    _bind_or_error,
)

from clickhouse_sqlalchemy.sql.selectable import (
    Join,
    Select,
)
from . import ddl


class Table(TableBase):
    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_ddl_visitor(ddl.SchemaDropper, self,
                              checkfirst=checkfirst, if_exists=if_exists)

    def join(self, right, onclause=None, isouter=False, full=False,
             type=None, strictness=None, distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)

    def select(self, whereclause=None, **params):
        return Select._create([self], whereclause, **params)

    @classmethod
    def _make_from_standard(cls, std_table, _extend_on=None):
        ch_table = cls(std_table.name, std_table.metadata)
        ch_table.schema = std_table.schema
        ch_table.fullname = std_table.fullname
        ch_table.implicit_returning = std_table.implicit_returning
        ch_table.comment = std_table.comment
        ch_table.info = std_table.info
        ch_table._prefixes = std_table._prefixes
        ch_table.dialect_options = std_table.dialect_options

        if _extend_on is None:
            ch_table._columns = std_table._columns

        return ch_table

    def _autoload(self, metadata, autoload_with, include_columns, **kwargs):
        rv = super(Table, self)._autoload(
            metadata, autoload_with, include_columns, **kwargs
        )
        autoload_with = _bind_or_error(
            metadata, msg="No engine is bound to this Table's MetaData."
        )
        insp = inspection.inspect(autoload_with)
        with insp._operation_context() as conn:
            autoload_with.dialect._reflect_engine(conn, self)
        return rv