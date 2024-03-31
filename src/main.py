import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from config import Settings
from src.tests import TestQueries
from src.core import manager, logger


def main() -> None:
    manager.connect(create_all=False)
    logger.info('Reading data...')
    TestQueries.inspect(manager, excel_path=Settings.DATA_PATH)
    logger.info('\n\n\tTests:\n')
    TestQueries.run_all(manager)
    

if __name__ == "__main__":
    main()
    logger.info('Exit (For more info check the logs [default root log.log])')
