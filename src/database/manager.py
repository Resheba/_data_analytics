from alchemynger import SyncManager

from config import Settings


manager: SyncManager = SyncManager(Settings.DB_DSN)
