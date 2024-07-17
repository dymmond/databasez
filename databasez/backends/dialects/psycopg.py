"""
All the unique changes for the databases package
with the custom Numeric as the deprecated pypostgresql
for backwards compatibility and to make sure the
package can go to SQLAlchemy 2.0+.
"""

from sqlalchemy import types, util
from sqlalchemy.dialects.postgresql.psycopg import dialect as _dialect_psycopg


class PGDialect_psycopg(_dialect_psycopg):
    colspecs = util.update_copy(
        _dialect_psycopg.colspecs,
        {
            types.Numeric: types.Numeric,
            types.Float: types.Float,
        },
    )


dialect = PGDialect_psycopg
