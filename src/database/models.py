from sqlalchemy import Column, Integer, Numeric, String, Text, TIMESTAMP, SmallInteger, Float

from src.core import manager

from .view import AVGTempMaterializedView


class DataORM(manager.Base):
    __tablename__ = 'data'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<DataORM(id={self.id})>"

    # datetime = Column(TIMESTAMP(timezone=True))
    # T = Column(Numeric)
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
