'''
View and Materialized View modlule for database
'''


from sqlalchemy import Select, text, TableClause
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table as table_
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler

from .utils import classproperty


class MaterializedView(DDLElement):
    name: str
    selectable: Select
    # table: TableClause = _table()

    @classproperty
    def table(cls) -> TableClause:
        t = table_(cls.name)
        t.columns._populate_separate_keys(
            col._make_proxy(t) for col in cls.selectable.selected_columns
        )
        return t
    
    @classmethod
    def refresh(cls, session: Session) -> None:
        session.execute(text('REFRESH MATERIALIZED VIEW %s;' % (
            cls.name
        )))


@compiler.compiles(MaterializedView)
def _create_m_view_compiler(element: MaterializedView, compiler: PGDDLCompiler, **kwargs) -> str:
    return "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s;" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


class View(DDLElement):
    name: str
    selectable: Select
    # table: TableClause = _table()

    @classproperty
    def table(cls) -> TableClause:
        t = table_(cls.name)
        t.columns._populate_separate_keys(
            col._make_proxy(t) for col in cls.selectable.selected_columns
        )
        return t
    

@compiler.compiles(View)
def _create_view_compiler(element: View, compiler: PGDDLCompiler, **kwargs) -> str:
    return "CREATE OR REPLACE VIEW %s AS %s;" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )
