'''
View and Materialized View modlule for database
'''


from sqlalchemy import Select, select, TableClause
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler


class AVGTempMaterializedView(DDLElement):
    name: str = 'my_stuff'
    selectable: Select = (select())

    @classmethod
    def table(cls) -> TableClause:
        t = table(cls.name)
        t.columns._populate_separate_keys(
            col._make_proxy(t) for col in cls.selectable.selected_columns
        )
        return t


@compiler.compiles(AVGTempMaterializedView)
def _create_view_compiler(element: AVGTempMaterializedView, compiler: PGDDLCompiler, **kwargs) -> str:
    return "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )
