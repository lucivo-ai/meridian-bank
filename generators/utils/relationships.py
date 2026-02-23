"""
Shared ID registry — maintains referential integrity across generators.
Each generator registers the IDs it creates, and other generators look up valid IDs.
Also provides the shared database engine and bulk insert utilities.
"""
import numpy as np
from sqlalchemy import create_engine, text
from generators.config import DB_URL, BATCH_SIZE

# ── Database engine (shared) ─────────────────────────────────
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_size=5, max_overflow=10)
    return _engine

def execute_sql_file(filepath):
    """Execute a SQL file against the database."""
    engine = get_engine()
    with open(filepath, 'r') as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

def bulk_insert(table_name, records, columns=None):
    """Bulk insert records into a table using COPY-style efficiency."""
    if not records:
        return
    engine = get_engine()
    if columns is None:
        columns = list(records[0].keys())

    placeholders = ', '.join([f':{col}' for col in columns])
    col_list = ', '.join(columns)
    sql = text(f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})")

    with engine.connect() as conn:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            conn.execute(sql, batch)
        conn.commit()

def bulk_insert_df(table_name, df, if_exists='append'):
    """Insert a pandas DataFrame into a table."""
    engine = get_engine()
    schema, table = table_name.rsplit('.', 1) if '.' in table_name else (None, table_name)
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False,
              method='multi', chunksize=BATCH_SIZE)

# ── ID Registry ──────────────────────────────────────────────
class IDRegistry:
    """Central registry of generated IDs for cross-generator FK lookups."""

    def __init__(self):
        self._store = {}

    def register(self, entity_type: str, ids: list):
        """Register a list of IDs for an entity type."""
        self._store[entity_type] = np.array(ids)

    def get_ids(self, entity_type: str) -> np.ndarray:
        """Get all registered IDs for an entity type."""
        if entity_type not in self._store:
            raise KeyError(f"No IDs registered for '{entity_type}'. "
                          f"Available: {list(self._store.keys())}")
        return self._store[entity_type]

    def get_random_ids(self, entity_type: str, n: int, rng: np.random.Generator = None) -> np.ndarray:
        """Get n random IDs from registered entity, with replacement."""
        ids = self.get_ids(entity_type)
        if rng is None:
            rng = np.random.default_rng()
        return rng.choice(ids, size=n, replace=True)

    def get_random_id(self, entity_type: str, rng: np.random.Generator = None):
        """Get a single random ID."""
        return self.get_random_ids(entity_type, 1, rng)[0]

    def count(self, entity_type: str) -> int:
        """Count registered IDs for an entity type."""
        return len(self.get_ids(entity_type))

    def summary(self) -> dict:
        """Return summary of registered entities and counts."""
        return {k: len(v) for k, v in self._store.items()}


# Global registry instance
registry = IDRegistry()
