from queries.orm import SyncORM
from queries.core import SyncCore
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))


# SyncCore.create_tables()
# SyncCore.insert_workers()
# SyncCore.select_workers()
# SyncCore.update_worker()

SyncORM.create_tables()
SyncORM.insert_workers()
SyncORM.select_workers()
SyncORM.update_worker()
