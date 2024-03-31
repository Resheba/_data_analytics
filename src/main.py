import os, sys, logging

sys.path.insert(1, os.path.join(sys.path[0], ".."))
logging.basicConfig(level=logging.INFO)

import pandas as pd
from sqlalchemy import inspect

from src.core import manager, setup_sql, setup_df
from config import Settings
from src.database import DataORM, AVGTempDayMaterializedView, AVGTempMonthMaterializedView, SnowDayCoverMaterializedView


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
        logging.info('Refresh materialized views')
        session.execute(AVGTempDayMaterializedView())
        session.execute(AVGTempMonthMaterializedView())
        session.execute(SnowDayCoverMaterializedView())
        session.commit()
        logging.info('Materialized views refreshed')

    logging.info('Get first 5 rows from materialized views')
    print(manager(manager[AVGTempDayMaterializedView.table].select.limit(5), scalars=False))
    print(manager(manager[AVGTempMonthMaterializedView.table].select.limit(5), scalars=False))
    print(manager(manager[SnowDayCoverMaterializedView.table].select.limit(5), scalars=False))
    logging.info('Data printed')
    

if __name__ == "__main__":
    main()
