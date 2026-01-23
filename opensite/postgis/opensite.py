import json
import logging
from opensite.postgis.base import PostGISBase
from opensite.logging.opensite import OpenSiteLogger

class OpenSitePostGIS(PostGISBase):
    def __init__(self, log_level=logging.INFO):
        super().__init__(log_level)
        self.log = OpenSiteLogger("OpenSitePostGIS", log_level)
        self._ensure_registry_exists()

    def _ensure_registry_exists(self):
        """Creates the master lookup table if it doesn't exist."""

        self.log.debug("Creating opensite_branch table")

        # Audit table for branch configuration state
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS opensite_branch (
            yml_hash TEXT PRIMARY KEY,
            branch_name TEXT NOT NULL,
            config_json JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        self.log.debug("Creating opensite_registry table")

        # Human-readable lookup for every node
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS opensite_registry (
            table_id TEXT PRIMARY KEY,
            human_name TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            yml_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

    def register_branch(self, branch_name, yml_hash, config_dict):
        """Stores the full configuration JSON for a specific hash."""

        self.log.debug(f"Registering branch in opensite_branch {yml_hash} {branch_name}")

        query = """
            INSERT INTO opensite_branch (yml_hash, branch_name, config_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (yml_hash) DO UPDATE SET
                config_json = EXCLUDED.config_json;
        """
        self.execute_query(query, (yml_hash, branch_name, json.dumps(config_dict)))

    def register_node(self, node, branch):
        """
        Inserts a node's table mapping into the registry.
        Expects node.database_table and branch.custom_properties['hash'] to exist.
        """
        table_id = getattr(node, 'database_table', None)
        human_name = node.name
        branch_name = branch.name
        yml_hash = branch.custom_properties.get('hash')

        if not table_id:
            raise ValueError(f"Node {human_name} has no database_table assigned.")

        self.log.debug(f"Registering node in opensite_registery {table_id} {human_name} {branch_name}")

        query = """
        INSERT INTO opensite_registry (table_id, human_name, branch_name, yml_hash)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (table_id) DO UPDATE SET
            human_name = EXCLUDED.human_name,
            branch_name = EXCLUDED.branch_name;
        """
        self.execute_query(query, (table_id, human_name, branch_name, yml_hash))
