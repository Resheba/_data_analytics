import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from config import Settings
from src.tests import TestQueries
from src.core import manager


def main() -> None:
    manager.connect(create_all=False)
    TestQueries.inspect(manager, excel_path=Settings.DATA_PATH)
    TestQueries.run_all(manager)
    

if __name__ == "__main__":
    main()
