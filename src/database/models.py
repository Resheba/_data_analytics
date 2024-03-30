from sqlalchemy import Column, Integer, Numeric, Select, String, Text, TIMESTAMP, SmallInteger, Float, Date
from sqlalchemy import select, cast, func

from src.core import manager

from .view import MaterializedView


class DataORM(manager.Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)

    datetime = Column(TIMESTAMP(timezone=True), primary_key=True)
    T = Column(Numeric)
    # Po = Column(Numeric)
    # P = Column(Numeric)
    # Pa = Column(Numeric)
    # U = Column(SmallInteger)
    # DD = Column(String)
    # Ff = Column(SmallInteger)
    # ff10 = Column(SmallInteger)
    # ff3 = Column(SmallInteger)
    # N = Column(String)
    # WW = Column(Text)
    # W1 = Column(Text)
    # W2 = Column(Text)
    # Tn = Column(Float)
    # Tx = Column(Float)
    # Cl = Column(Text)
    # Nh = Column(Text)
    # H = Column(Text)
    # Cm = Column(Text)
    # Ch = Column(Text)
    # VV = Column(Text)
    # Td = Column(Float)
    # RRR = Column(Text)
    # tR = Column(Float)
    # E = Column(Text)
    # Tg = Column(Float)
    # E_ = Column("E'", Text)
    # sss = Column(Text)

    def __repr__(self) -> str:
        return f"<DataORM(id={self.id})>"


class AVGTempDayMaterializedView(MaterializedView):
    name: str = 'avg_temp_day'
    selectable: Select = (select(cast(DataORM.datetime, Date).label('date'), func.ROUND(func.AVG(DataORM.T), 2).label('avg_temp')).
                          group_by('date').
                          order_by('date')
                          )


# class AVGTempMonth
