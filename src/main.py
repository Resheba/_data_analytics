import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pandas as pd
from sqlalchemy import inspect, func, select
from pprint import pprint, pformat

from config import Settings
from src.core import manager, setup_sql, setup_df, logger
from src.database import DataORM, AVGTempDayMaterializedView, AVGTempMonthView, SnowDayCoverMaterializedView, SnowDayProYearView


def main() -> None:
    manager.connect(create_all=False)

    logger.info(f'Checking the existance of a `{DataORM.__tablename__}` table')
    if not inspect(manager.engine).has_table(DataORM.__tablename__):
        logger.info('Table not found. Prepare data')
        df: pd.DataFrame = pd.read_excel(Settings.DATA_PATH)
        logger.info(f'Data from `{Settings.DATA_PATH}` has been read')
        df: pd.DataFrame = setup_df(df)
        logger.info(f'Data from `{Settings.DATA_PATH}` has been prepared')
        setup_sql(df, DataORM.__tablename__, manager.engine)
        logger.info('Data prepared')

    with manager.get_session() as session:
        logger.info('Refresh views')
        session.execute(AVGTempDayMaterializedView())
        session.execute(AVGTempMonthView())
        session.execute(SnowDayCoverMaterializedView())
        session.execute(SnowDayProYearView())
        session.commit()
        logger.info('Views refreshed')

    logger.info('\n\n\tResults:\n')

    logger.info('AVG Temperature by day')
    logger.info(pformat(
        manager(
            manager[AVGTempDayMaterializedView.table].select.limit(5), scalars=False
        )
    ))
    logger.info('AVG Temperature by month')
    logger.info(pformat(
        manager(
            manager[AVGTempMonthView.table].select.limit(5), scalars=False
        )
    ))
    logger.info('Days with snow cover')
    logger.info(pformat(
        manager(
            select(func.count()).where(SnowDayCoverMaterializedView.table.c.snow_cover != 0)
        )
    ))
    logger.info('Snow cover by years')
    logger.info(pformat(
        manager(
            manager[SnowDayProYearView.table].select, scalars=False
        )
    ))
    

if __name__ == "__main__":
    main()
