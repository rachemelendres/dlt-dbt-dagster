from dagster import Definitions
from dagster.components import definitions, load_defs

import dlt_dbt_dagster.defs


@definitions
def defs() -> Definitions:
    return load_defs(defs_root=dlt_dbt_dagster.defs)
