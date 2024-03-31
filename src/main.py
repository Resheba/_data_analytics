import os, sys, logging

sys.path.insert(1, os.path.join(sys.path[0], ".."))
logging.basicConfig(level=logging.INFO)

import pandas as pd
from sqlalchemy import inspect, func, select

from src.core import manager, setup_sql, setup_df
from config import Settings
from src.database import DataORM, AVGTempDayMaterializedView, AVGTempMonthView, SnowDayCoverMaterializedView


def main() -> None:
    manager.connect(create_all=False)

    logging.info(f'Checking the existance of a `{DataORM.__tablename__}` table')
    if not inspect(manager.engine).has_table(DataORM.__tablename__):
        logging.info('Table not found. Prepare data')
        df: pd.DataFrame = pd.read_excel(Settings.DATA_PATH)
        logging.info(f'Data from `{Settings.DATA_PATH}` has been read')
        df: pd.DataFrame = setup_df(df)
        logging.info(f'Data from `{Settings.DATA_PATH}` has been prepared')
        setup_sql(df, DataORM.__tablename__, manager.engine)
        logging.info('Data prepared')

    with manager.get_session() as session:
        logging.info('Refresh views')
        session.execute(AVGTempDayMaterializedView())
        session.execute(AVGTempMonthView())
        session.execute(SnowDayCoverMaterializedView())
        session.commit()
        logging.info('Views refreshed')

    logging.info('\n\n\tResults:\n')

    logging.info('AVG Temperature by day')
    print(
        manager(
            manager[AVGTempDayMaterializedView.table].select.limit(5), scalars=False
        )
    )
    logging.info('AVG Temperature by month')
    print(
        manager(
            manager[AVGTempMonthView.table].select.limit(5), scalars=False
        )
    )
    logging.info('Days with snow cover')
    print(
        manager(
            select(func.count()).where(SnowDayCoverMaterializedView.table.c.snow_cover != 0)
        )
    )
    logging.info('')
    

if __name__ == "__main__":
    main()
