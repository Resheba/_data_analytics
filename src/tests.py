from pprint import pformat
from pandas import DataFrame, read_excel
from sqlalchemy import select, func, inspect
from alchemynger import SyncManager

from config import Settings

from .core import logger, setup_df, setup_sql
from .database import (
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


class TestQueries:
    @classmethod
    def inspect(cls, manager: SyncManager, *, excel_path: str | None = ...):
        logger.info(f'Checking the existance of a `{DataORM.__tablename__}` table')
        if not inspect(manager.engine).has_table(DataORM.__tablename__):
            if excel_path is None or excel_path is Ellipsis:
                excel_path = Settings.DATA_PATH
            df: DataFrame = read_excel(excel_path)
            logger.info('Table not found. Prepare data')
            logger.info(f'Data from `{excel_path}` has been read')
            df: DataFrame = setup_df(df)
            logger.info(f'Data from `{excel_path}` has been prepared')
            setup_sql(df, DataORM.__tablename__, manager.engine)
            logger.info('Data prepared')
        else:
            logger.info(f'Table {DataORM.__tablename__} is already exists')

        with manager.get_session() as session:
            logger.info('Create views')
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
            logger.info('Views created')

    @classmethod
    def run_all(cls, manager: SyncManager):
        cls.test_avg_temp_day(manager)
        cls.test_avg_temp_month(manager)
        cls.test_snow_day_count(manager)
        cls.test_snow_day_pro_year(manager)
        cls.test_max_snow_cover_season_1(manager)
        cls.test_max_snow_cover_season_2(manager)
        cls.test_days_with_snow_cover_season(manager)
        cls.test_snow_fall_window_function(manager)
        cls.test_snow_fall_lateral_join(manager)

    @classmethod
    def test_avg_temp_day(cls, manager: SyncManager):
        logger.info('Средняя температура по дням')
        logger.info('\n' + pformat(
            manager(
                manager[AVGTempDayView.table].select.limit(5), scalars=False
            )
        ))

    @classmethod
    def test_avg_temp_month(cls, manager: SyncManager):
        logger.info('Средняя температура по месяцам')
        logger.info('\n' + pformat(
            manager(
                manager[AVGTempMonthView.table].select.limit(5), scalars=False
            )
        ))

    @classmethod
    def test_snow_day_count(cls, manager: SyncManager):
        logger.info('Количество дней со снежным покровом (всего)')
        logger.info('\n' + pformat(
            manager(
                select(func.count()).where(SnowDayCoverView.table.c.snow_cover != 0)
            )
        ))

    @classmethod
    def test_snow_day_pro_year(cls, manager: SyncManager):
        logger.info('Количество дней со снежным покровом (погодовой)')
        logger.info('\n' + pformat(
            manager(
                manager[SnowDayProYearView.table].select, scalars=False
            )
        ))

    @classmethod
    def test_max_snow_cover_season_2(cls, manager: SyncManager):
        logger.info('Максимальная высота покрова снега по сезонам V2')
        logger.info('\n' + pformat(
            manager(
                manager[MaxSnowCoverSeason2MaterializedView.table].select, scalars=False
            )
        ))

    @classmethod
    def test_max_snow_cover_season_1(cls, manager: SyncManager):
        logger.info('Максимальная высота покрова снега по сезонам V1')
        logger.info('\n' + pformat(
            manager(
                manager[MaxSnowCoverSeason1MaterializedView.table].select, scalars=False
            )
        ))

    @classmethod
    def test_days_with_snow_cover_season(cls, manager: SyncManager):
        logger.info('Количество дней со снежным покровом в сезоны')
        logger.info('\n' + pformat(
            manager(
                manager[DaysWithSnowCoverSeasonMaterializedView.table].select, scalars=False
            )
        ))

    @classmethod
    def test_snow_fall_lateral_join(cls, manager: SyncManager):
        logger.info('Даты схода (начала убывания уровня снега) Lateral Join')
        logger.info('\n' + pformat(
            manager(
                manager[SnowFallLJMaterializedView.table].select, scalars=False
            )
        ))

    @classmethod
    def test_snow_fall_window_function(cls, manager: SyncManager):
        logger.info('Даты схода (начала убывания уровня снега) Window Function')
        logger.info('\n' + pformat(
            manager(
                manager[SnowFallWFView.table].select, scalars=False
            )
        ))
