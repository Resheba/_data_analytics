import os
from typing import Final
from dotenv import load_dotenv

load_dotenv(override=True)


class Settings:
    DB_DSN: Final[str] = os.getenv("DB_DSN")
    DATA_PATH: Final[str] = os.getenv("DATA_PATH")


if not all(value for key, value in Settings.__dict__.items() if not key.startswith("__")):
    raise ValueError(
        "Environment variables are not set:\n\t"
        +'\n\t'.join(
        name for name in Settings.__dict__.keys() 
        if not name.startswith('__') and Settings.__dict__.get(name) is None
        ))
