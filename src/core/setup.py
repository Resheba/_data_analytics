from sqlalchemy import Engine
from pandas import DataFrame, to_datetime
from sqlalchemy.dialects.postgresql import TIMESTAMP, NUMERIC, SMALLINT, VARCHAR


def setup_sql(df: DataFrame, tablename: str, engine: Engine) -> None:
    df.to_sql(
        name=tablename, 
        con=engine, 
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
    

def setup_df(df: DataFrame) -> DataFrame:
    df[df.columns[0]] = df[df.columns[0]].astype(str) + 'T' + df[df.columns[1]].astype(str) + 'Z+03:00'
    del df[df.columns[1]]
    df[df.columns[0]] = to_datetime(df[df.columns[0]])
    df.rename(
        columns={
            df.columns[0]: "datetime",
            }, 
            inplace=True)
    return df
