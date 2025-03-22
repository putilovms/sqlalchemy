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
SyncORM.insert_resumes()
SyncORM.select_resumes_avg_compensation()
SyncORM.insert_additional_resumes()
SyncORM.join_cte_subquery_window_func()
SyncORM.select_workers_with_lazy_relationship()
SyncORM.select_workers_with_joined_relationship()
SyncORM.select_workers_with_selectin_relationship()
# SyncORM.select_workers_with_condition_relationship()
# SyncORM.select_workers_relationship_contains_eager()
# SyncORM.select_workers_relationship_contains_eager_with_limit()
