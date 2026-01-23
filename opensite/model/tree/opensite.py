import os
import json
import yaml
import logging
from typing import Dict, Any, List, Optional
from .base import Tree
from ..node import Node
from opensite.constants import OpenSiteConstants
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.logging.opensite import OpenSiteLogger
from opensite.ckan.opensite import OpenSiteCKAN

class OpenSiteTree(Tree):

    def __init__(self, overrides=None, log_level=logging.INFO):
        super().__init__(overrides)

        self.log = OpenSiteLogger("OpenSiteTree", log_level)
        self.db = OpenSitePostGIS()

        self.log.info("Tree initialized and ready.")
        
    def register_to_database(self):
        """Syncs the tree structure to PostGIS using the logger for feedback."""
        self.log.info("Starting database synchronization...")

        def _recurse_and_register(node, branch):
            # Use debug for high-volume mapping logs (White)
            self.log.debug(f"Mapping node: {node.name} -> {node.database_table}")
            self.db.register_node(node, branch)
            
            for child in node.children:
                _recurse_and_register(child, branch)

        for branch in self.root.children:
            yml_hash = branch.custom_properties.get('hash')
            if yml_hash:
                # Use info for major milestones (Blue)
                self.log.info(f"Syncing branch: {branch.name} [{yml_hash[:8]}]")
                
                try:
                    self.db.register_branch(branch.name, yml_hash, branch.custom_properties)
                    _recurse_and_register(branch, branch)
                except Exception as e:
                    # Use error for failures (Red)
                    self.log.error(f"Failed to sync branch {branch.name}: {e}")

        self.log.info("Database synchronization complete.")

    def convert_name_to_title(self, name: str) -> str:
        """
        'railway-lines--uk' -> 'Railway Lines'
        'hazard-zone--exclusion--restricted' -> 'Hazard Zone - Exclusion - Restricted'
        """

        # REDUNDANT - We use CKAN to get correct title
        lowercase_words = [" And ", " Of ", " From "]

        if not name:
            return ""

        delete_area = ['uk', 'gb', 'eu']

        # Split on double hyphen
        parts = name.split("--")
        
        # Filter and process
        cleaned_parts = []
        for part in parts:
            if part.lower() not in delete_area:
                # Replace single hyphens with spaces
                words = part.replace("-", " ").split()
                capitalized_words = " ".join([w.capitalize() for w in words])
                cleaned_parts.append(capitalized_words)

        title = " - ".join(cleaned_parts)
        for lowercase_word in lowercase_words: title = title.replace(lowercase_word, lowercase_word.lower())

        return title

    def get_math_context(self, branch: Node) -> Dict[str, float]:
        """
        Builds a math context specific to the properties 
        stored on this branch's root node.
        """
        all_props = branch.custom_properties
        function_keys = OpenSiteConstants.TREE_BRANCH_PROPERTIES.get('functions', [])
        
        return {
            k: v for k, v in all_props.items() 
            if k in function_keys and isinstance(v, (int, float, str))
        }

    def add_yaml(self, filepath: str):
        """Loads a YAML file and triggers the branch-specific enrichment logic."""
        # 1. Use the base class to load the raw file structure into a branch
        super().add_yaml(filepath)
        
        if not self.root.children: return False
            
        # 2. Get the branch we just created (the last child of the root)
        current_branch = self.root.children[-1]

        # 3. Trigger the unified enrichment logic
        # This now handles property mapping, math, and surgical pruning
        self.enrich_branch(current_branch)

        self.register_to_database()

        return True

    def resolve_branch_math(self, branch: Node):
        # Get context dynamically via our new function
        context = self.get_math_context()
        
        def walk(node: Node):
            for k, v in node.custom_properties.items():
                if isinstance(v, str):
                    # base.py's resolve_math handles the calculation
                    node.custom_properties[k] = self.resolve_math(v, context)
            
            for child in node.children:
                walk(child)

        walk(branch)

    def enrich_branch(self, branch: Node):
        """
        Merges file data with global defaults and prunes
        """
        
        self.log.debug(f"Running enrich_branch")

        all_registry_keys = [k for sub in OpenSiteConstants.TREE_BRANCH_PROPERTIES.values() for k in sub]

        for key in all_registry_keys:
            # 1. Try to find the node in the current YAML branch
            prop_node = self.find_child(branch, key)
            
            # 2. Determine value: Local Node > Global Default
            val = None
            if prop_node:
                val = prop_node.custom_properties.get('value')
            else:
                val = self._defaults.get(key)

            # 3. Apply to Branch Node
            if val is not None:
                if key == 'title':
                    branch.title = val
                else:
                    branch.custom_properties[key] = val

        # 3. Get math context FROM the branch properties we just set
        context = self.get_math_context(branch)
        
        # 4. Locate structure/style/buffer roots
        struct_root = self.find_child(branch, "structure")
        style_root = self.find_child(branch, "style")
        buffer_root = self.find_child(branch, "buffers")

        if not struct_root:
            # Cleanup if no structure (deletes osm, tip-height nodes etc.)
            for child in list(branch.children):
                self.delete_node(child)
            return

        # 5. Enrichment Loop (Math & Style)
        for category_node in struct_root.children:
            category_node.node_type = None  
            
            # Apply Style
            if style_root:
                style_match = self.find_child(style_root, category_node.name)
                if style_match:
                    category_node.style = {
                        c.name: c.custom_properties.get('value') 
                        for c in style_match.children
                    }

            # Apply Buffers & Resolve Math
            for dataset_node in category_node.children:
                dataset_node.node_type = "source"
                if buffer_root:
                    buf_node = self.find_child(buffer_root, dataset_node.name)
                    if buf_node:
                        val = buf_node.custom_properties.get('value')
                        dataset_node.database_action = "buffer"
                        # Math resolution uses the branch-specific context
                        dataset_node.custom_properties['buffer_value'] = self.resolve_math(val, context)

        # 6. Sibling Cleanup
        # Deletes all original YAML nodes (tip-height, title, style, etc.)
        extraneous_nodes = self.get_siblings(struct_root)
        for node in extraneous_nodes:
            self.delete_node(node)

        # 7. Final Promotion
        valid_data_nodes = list(struct_root.children)
        for node in valid_data_nodes:
            node.parent = branch
            branch.children.append(node)

        self.delete_node(struct_root)

        # self._apply_titles_recursive(branch)

    def _apply_titles_recursive(self, node: Node):
        """Walks down the tree and sets titles if they are currently just the name."""

        # If title is missing or still matches the raw name, format it
        if not node.title or node.title == node.name:
            node.title = self.convert_name_to_title(node.name)
        
        self.log.debug(f"Running _apply_titles_recursive: {node.name} --> '{node.title}'")

        for child in node.children:
            self._apply_titles_recursive(child)

    def choose_priority_resource(self, resources, priority_ordered_formats):
        """
        Choose the single best dataset from a list based on FORMATS priority.
        """
        if not resources:
            return None
        
        # We want to find the dataset whose resource format has the lowest index in self.FORMATS
        best_resource = resources[0] # Default to first if no priority match found
        best_index = len(priority_ordered_formats)

        for resource in resources:
            format = resource.get('format')
            if format in priority_ordered_formats:
                current_index = priority_ordered_formats.index(format)
                if current_index < best_index:
                    best_index = current_index
                    best_resource = resource
                    
                    # Optimization: If we found the #1 priority (GPKG), we can stop looking
                    if best_index == 0:
                        return best_resource

        return best_resource
        
    def update_metadata(self, ckan: OpenSiteCKAN):
        """
        Syncs titles and URLs for both Groups and Datasets across the entire tree.
        """
        self.log.info("Synchronizing node titles with CKAN metadata...")

        model = ckan.query()
        ckan_base = ckan.url

        # Build a unified lookup map for both Groups and Datasets
        ckan_lookup = {}
        
        for group_name, data in model.items():
            # Add the group itself to the lookup (if it's not the 'default' catch-all)
            # This allows folders in your tree to get their Titles from CKAN groups
            if group_name != 'default':
                ckan_lookup[group_name] = {
                    'title': data.get('group_title', group_name).strip(),
                    # 'url': f"{ckan_base}/group/{group_name}"
                }

            # Add priority resource within each dataset
            for dataset in data.get('datasets', []):
                priority_resource = self.choose_priority_resource(dataset.get('resources', []), ckan.FORMATS)
                print(json.dumps(priority_resource, indent=4))
                package_name = dataset.get('package_name', '')
                if package_name:
                    ckan_lookup[package_name] = {
                        'title': dataset.get('title').strip(), 
                        'url': priority_resource.get('url').strip(),
                        'format': priority_resource.get('format').strip()
                    }
                
        # Recursive walker (unchanged logic, now with better data)
        def walk_and_update(node):
            matches = 0
            if node.name in ckan_lookup:
                meta = ckan_lookup[node.name]
                node.title = meta['title']
                if 'url' in meta: node.url = meta['url']
                if 'format' in meta: node.format = meta['format'] 
                matches += 1
            
            if hasattr(node, 'children'):
                for child in node.children:
                    matches += walk_and_update(child)
            return matches

        # Execute
        total_matches = walk_and_update(self.root)
        self.log.info(f"Metadata sync complete. Updated {total_matches} total nodes.")