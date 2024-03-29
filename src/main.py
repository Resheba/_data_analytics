import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import logging
import pandas as pd

from sqlalchemy.dialects.postgresql import (
    VARCHAR, 
    TIMESTAMP, 
    NUMERIC, 
    SMALLINT,
)

from src.database import manager

def setup_df(df: pd.DataFrame) -> pd.DataFrame:
    df[df.columns[0]] = df[df.columns[0]].astype(str) + 'T' + df[df.columns[1]].astype(str) + 'Z'
    del df[df.columns[1]]
    df.rename(
        columns={
            df.columns[0]: "datetime",
            df.columns[1]: "temperature",
            df.columns[6]: 'windiness',
            df.columns[10]: "cloud_cover"
            }, 
            inplace=True)
    return df


def setup_sql(df: pd.DataFrame) -> None:
    df.to_sql(
        name="subline", 
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
        
        index=False
            )


def main() -> None:
    manager.connect(create_all=False)
    df: pd.DataFrame = pd.read_excel("./data/данные.xlsx", parse_dates=True)
    df: pd.DataFrame = setup_df(df)
    setup_sql(df)


if __name__ == "__main__":
    main()
