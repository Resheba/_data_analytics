import os, sys, logging

sys.path.insert(1, os.path.join(sys.path[0], ".."))
logging.basicConfig(level=logging.INFO)

import pandas as pd
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import (
    VARCHAR, 
    TIMESTAMP, 
    NUMERIC, 
    SMALLINT,
    INTEGER,
)

from src.core import manager
from src.database import DataORM


def setup_df(df: pd.DataFrame) -> pd.DataFrame:
    df[df.columns[0]] = df[df.columns[0]].astype(str) + ' ' + df[df.columns[1]].astype(str) + 'Z+03:00'
    del df[df.columns[1]]
    df[df.columns[0]] = pd.to_datetime(df[df.columns[0]])
    df.rename(
        columns={
            df.columns[0]: "datetime",
            }, 
            inplace=True)
    return df


def setup_sql(df: pd.DataFrame) -> None:
    df.to_sql(
        name=DataORM.__tablename__, 
        con=manager.engine, 
        if_exists="replace", 
        dtype={
            df.columns[0]: TIMESTAMP(timezone=True),
            df.columns[1]: NUMERIC(scale=2),
            df.columns[2]: NUMERIC(scale=1),
            df.columns[3]: NUMERIC(scale=1),
            df.columns[4]: NUMERIC(scale=1),
            df.columns[5]: SMALLINT,
            df.columns[6]: VARCHAR,
            df.columns[7]: SMALLINT,
            df.columns[8]: SMALLINT,
            df.columns[9]: SMALLINT,
            df.columns[10]: VARCHAR,
            },
        
        index=True,
        index_label="id",
            )


def main() -> None:
    manager.connect(create_all=False)
    # df: pd.DataFrame = pd.read_excel("./data/данные.xlsx")
    # df: pd.DataFrame = setup_df(df)
    # setup_sql(df)
    print(manager.execute(manager[DataORM].select.limit(20)))


if __name__ == "__main__":
    main()
