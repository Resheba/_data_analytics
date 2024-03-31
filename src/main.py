import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pandas as pd
from pprint import pformat
from sqlalchemy import inspect, func, select

from config import Settings
from src.core import manager, setup_sql, setup_df, logger
from src.database import (
        DataORM, 
        AVGTempDayView, 
        AVGTempMonthView, 
        SnowDayCoverView, 
        SnowDayProYearView, 
        MaxSnowCoverSeason1MaterializedView, 
        MaxSnowCoverSeason2MaterializedView,
        DaysWithSnowCoverSeasonMaterializedView,
        SnowFallLJMaterializedView,
        SnowFallWFView
    )


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
    else:
        logger.info(f'Table {DataORM.__tablename__} is already exists')

    with manager.get_session() as session:
        logger.info('Refresh views')
        session.execute(AVGTempDayView())
        session.execute(AVGTempMonthView())
        session.execute(SnowDayCoverView())
        session.execute(SnowDayProYearView())
        session.execute(MaxSnowCoverSeason1MaterializedView())
        session.execute(MaxSnowCoverSeason2MaterializedView())
        session.execute(DaysWithSnowCoverSeasonMaterializedView())
        session.execute(SnowFallLJMaterializedView())
        session.execute(SnowFallWFView())
        session.commit()
        logger.info('Views refreshed')

    logger.info('\n\n\tResults:\n')

    logger.info('Средняя температура по дням')
    logger.info('\n' + pformat(
        manager(
            manager[AVGTempDayView.table].select.limit(5), scalars=False
        )
    ))
    logger.info('Средняя температура по месяцам')
    logger.info('\n' + pformat(
        manager(
            manager[AVGTempMonthView.table].select.limit(5), scalars=False
        )
    ))
    logger.info('Количество дней со снежным покровом (всего)')
    logger.info('\n' + pformat(
        manager(
            select(func.count()).where(SnowDayCoverView.table.c.snow_cover != 0)
        )
    ))
    logger.info('Количество дней со снежным покровом (погодовой)')
    logger.info('\n' + pformat(
        manager(
            manager[SnowDayProYearView.table].select, scalars=False
        )
    ))
    logger.info('Максимальный покров снега по сезонам V2')
    logger.info('\n' + pformat(
        manager(
            manager[MaxSnowCoverSeason2MaterializedView.table].select, scalars=False
        )
    ))
    logger.info('Максимальный покров снега по сезонам V1')
    logger.info('\n' + pformat(
        manager(
            manager[MaxSnowCoverSeason1MaterializedView.table].select, scalars=False
        )
    ))
    logger.info('Количество дней со снежным покровом в сезноны')
    logger.info('\n' + pformat(
        manager(
            manager[DaysWithSnowCoverSeasonMaterializedView.table].select, scalars=False
        )
    ))
    logger.info('Даты схода (начала убывания уровня снега) Lateral Join')
    logger.info('\n' + pformat(
        manager(
            manager[SnowFallLJMaterializedView.table].select, scalars=False
        )
    ))
    logger.info('Даты схода (начала убывания уровня снега) Window Function')
    logger.info('\n' + pformat(
        manager(
            manager[SnowFallWFView.table].select, scalars=False
        )
    ))
    

if __name__ == "__main__":
    main()
    logger.info('Exit')
