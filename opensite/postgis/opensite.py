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
            completed BOOLEAN DEFAULT FALSE,
            table_id TEXT PRIMARY KEY,
            human_name TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            yml_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

    def sync_registry(self):
        """
        Synchronizes registry, physical tables, and branch metadata.
        """
        self.log.info("Starting registry synchronization...")
        
        # 1. Get current registry state
        registry_entries = self.fetch_all("SELECT table_id, completed FROM opensite_registry")
        registry_names = {row['table_id'] for row in registry_entries}
        
        # 2. Get physical tables
        protected_tables = {
            'opensite_registry', 'opensite_branch', 'spatial_ref_sys', 
            'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overview'
        }
        physical_tables = {t for t in self.get_table_names() if t not in protected_tables}

        # --- Step A & B: Clean the Registry ---
        for entry in registry_entries:
            table_id = entry['table_id']
            completed = entry.get('completed')

            if not completed:
                self.log.debug(f"Removing incomplete registry entry: {table_id}")
                self.execute_query("DELETE FROM opensite_registry WHERE table_id = %s", (table_id,))
                registry_names.discard(table_id)
                continue

            if table_id not in physical_tables:
                self.log.debug(f"Removing orphaned registry entry (no table found): {table_id}")
                self.execute_query("DELETE FROM opensite_registry WHERE table_id = %s", (table_id,))
                registry_names.discard(table_id)

        # --- Step C: Clean the Database (Untracked Tables) ---
        for table_id in physical_tables:
            if table_id not in registry_names:
                self.log.warning(f"Dropping untracked table: {table_id}")
                self.execute_query(f'DROP TABLE IF EXISTS "{table_id}" CASCADE')

        # --- Step D: Clean the Branches ---
        # We look for branch_name in opensite_branch that no longer has 
        # ANY associated records in opensite_registry
        self.log.info("Checking for orphaned branches...")
        
        orphaned_branches_sql = """
            SELECT b.branch_name 
            FROM opensite_branch b
            LEFT JOIN opensite_registry r ON b.branch_name = r.branch_name
            WHERE r.branch_name IS NULL
        """
        orphaned_branches = self.fetch_all(orphaned_branches_sql)
        
        for branch in orphaned_branches:
            b_name = branch['branch_name']
            self.log.warning(f"Removing orphaned branch metadata: {b_name}")
            self.execute_query("DELETE FROM opensite_branch WHERE branch_name = %s", (b_name,))

        self.log.info("Registry and branch synchronization complete.")

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
        Expects node.output and branch.custom_properties['hash'] to exist.
        """
        output = getattr(node, 'output', None)
        human_name = node.name
        branch_name = branch.name
        yml_hash = branch.custom_properties.get('hash')

        if output:
            self.log.debug(f"Registering node in opensite_registery {output} {human_name} {branch_name}")

            query = """
            INSERT INTO opensite_registry (table_id, human_name, branch_name, yml_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_id) DO UPDATE SET
                human_name = EXCLUDED.human_name,
                branch_name = EXCLUDED.branch_name;
            """
            self.execute_query(query, (output, human_name, branch_name, yml_hash))
