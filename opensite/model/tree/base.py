import yaml
import os
import json
import hashlib
import logging
from typing import Optional, Dict, Any, List
from ..node import Node
from opensite.logging.opensite import LoggingBase

class Tree:

    DEFAULT_YML = 'defaults.yml'
    TREE_BRANCH_PROPERTIES = {}
    OUTPUT_FIELDS = [
        "urn", 
        "name", 
        "title", 
        "node_type", 
        "url", 
        "format", 
        "database_table", 
        "database_action", 
        "style", 
        "custom_properties", 
        "status", 
        "dependencies", 
        "log"
    ]

    OUTPUT_FIELDS = ['name', 'title', 'url', 'format']
    
    def __init__(self, overrides: dict = None, log_level=logging.INFO):
        self.log = LoggingBase("Tree", log_level)
        self._nodes_by_urn: Dict[int, Any] = {}
        self._urn_counter = 1
        self._overrides = overrides or {}
        self._defaults = {}
        self.load_defaults(self.DEFAULT_YML)
        if self._overrides: self._defaults.update(self._overrides)
        self.root = self.create_node("root", node_type="root")

    def load_defaults(self, filepath: str):
        """Loads a YAML file directly into the defaults dictionary."""
        if not os.path.exists(filepath):
            self.log.warning(f"Warning: Defaults file not found at {filepath}")
            return

        with open(filepath, 'r') as f:
            try:
                raw_data = yaml.safe_load(f)
                if not raw_data:
                    return

                # We only want to store keys defined in our registry
                all_keys = [k for sub in self.TREE_BRANCH_PROPERTIES.values() for k in sub]
                
                for key in all_keys:
                    if key in raw_data:
                        # Store the value directly (no nodes involved)
                        self._defaults[key] = raw_data[key]
                        
            except yaml.YAMLError as e:
                self.log.error(f"Error parsing defaults.yml: {e}")

    def create_node(self, name: str, **kwargs) -> Node:
        urn = self._urn_counter
        self._urn_counter += 1
        node = Node(urn=urn, name=name, **kwargs)
        self._nodes_by_urn[urn] = node
        return node

    def delete_node(self, node: Node):
        """
        Permanently removes a node and all its descendants from the tree 
        and the URN registry.
        """
        if not node:
            return

        # Recursive helper to unregister URNs
        def unregister_recursive(n: Node):
            if n.urn in self._nodes_by_urn:
                del self._nodes_by_urn[n.urn]
            for child in n.children:
                unregister_recursive(child)

        # Wipe the URNs for this node and all its children
        unregister_recursive(node)

        # Sever the connection from the parent
        if node.parent:
            try:
                node.parent.children.remove(node)
            except ValueError:
                # Node wasn't in parent's list, already detached
                pass
        
        # 4. Clear the node's own references to be safe
        node.parent = None
        node.children = []

    def find_node(self, name: str, start_node: Optional[Node] = None) -> Optional[Node]:
        """Recursive search for a node by name."""
        current = start_node or self.root
        if current.name == name:
            return current
        for child in current.children:
            result = self.find_node(name, child)
            if result:
                return result
        return None

    def find_child(self, parent: Node, name: str) -> Optional[Node]:
        """Non-recursive search for a direct child."""
        for child in parent.children:
            if child.name == name:
                return child
        return None

    def get_siblings(self, node: Node) -> List[Node]:
        """Returns a list of all nodes at the same level as the given node."""
        if not node or not node.parent:
            return []
        # Return all children of the parent except the node itself
        return [child for child in node.parent.children if child != node]

    def prune_node(self, node: Node):
        """Removes node from its parent."""
        if node.parent:
            node.parent.children.remove(node)

    def to_json(self) -> Dict[str, Any]:
        """
        Returns the tree as a JSON-compatible dictionary object.
        """
        return self._node_to_dict(self.root)

    def to_list(self, node: Optional[Node] = None, depth: int = 0) -> List[Dict[str, Any]]:
        """
        Flattens the tree into a list of dictionaries, 
        adding 'depth' as the first field.
        """
        current = node or self.root
        if not current:
            return []

        # Create the dict starting with depth
        node_dict = {"depth": depth}
        
        # Pull all attributes from the node
        for field in self.OUTPUT_FIELDS:
            node_dict[field] = getattr(current, field, None)

        # 2. Start the list with the current node
        flat_list = [node_dict]

        # 3. Recursively add children, incrementing depth
        for child in current.children:
            flat_list.extend(self.to_list(child, depth + 1))

        return flat_list
    
    def _node_to_dict(self, node: Node) -> Dict[str, Any]:
        """Recursive helper to build the full JSON object."""
        if not node:
            return {}

        # Use a dictionary comprehension to dump the data
        data = {field: getattr(node, field, None) for field in self.OUTPUT_FIELDS}

        # Ensure custom_properties is at least an empty dict if getattr returns None
        if "custom_properties" in data:
            if data["custom_properties"] is None:
                data["custom_properties"] = {}

        # Recursively add children
        data["children"] = [self._node_to_dict(child) for child in node.children]
        
        return data

    def load_yaml(self, filepath: str):
        """Clears existing branches (below root) and loads fresh."""
        self.root.children = []
        # Reset the URN lookup to just the root
        self._nodes_by_urn = {self.root.urn: self.root}
        self.add_yaml(filepath)

    def set_node_table_names(self, node, branch):
        """Recursively sets the database_table field for a node and its children."""
        node.database_table = self.get_table_name(node, branch)
        self.log.debug(f"Setting database_table of {node.name} to {node.database_table}")

        for child in node.children:
            self.set_node_table_names(child, branch)

    def add_yaml(self, filepath: str):
        """Adds a YAML file as a new sibling branch under the root."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"YAML not found: {filepath}")

        with open(filepath, 'r') as f:
            data = yaml.safe_load(f)

        if not data: return False

        processed_data = data.copy()

        # Apply overrides 
        if self._overrides:
            processed_data.update(self._overrides)

        # Generate the unique hash for this specific state
        # We use sort_keys to ensure consistent hashing regardless of dictionary order
        state_string = json.dumps(processed_data, sort_keys=True).encode('utf-8')
        state_hash = hashlib.md5(state_string).hexdigest()

        # Create a branch container for this file
        branch_name = os.path.basename(filepath)
        branch_node = self.create_node(branch_name, node_type="branch")
        branch_node.parent = self.root
        branch_node.custom_properties['yml'] = processed_data
        branch_node.custom_properties['hash'] = state_hash
        self.root.children.append(branch_node)

        # Build raw structure into this branch
        self.build_from_dict(processed_data, branch_node)

        self.set_node_table_names(branch_node, branch_node)

        return True

    def add_yamls(self, yaml_paths: list):
        """
        Batch processes a list of file paths.
        """
        self.log.info(f"Batch processing {len(yaml_paths)} YAML files...")
        
        results = []
        for path in yaml_paths:
            # We call the existing single-file logic
            success = self.add_yaml(path)
            if success:
                results.append(path)
        
        self.log.info(f"Successfully added {len(results)}/{len(yaml_paths)} files to tree.")

        return results

    def build_from_dict(self, data: Any, parent_node: Node):
        """Standard recursive dictionary-to-node mapper."""
        if isinstance(data, dict):
            for key, value in data.items():
                new_node = self.create_node(name=str(key))
                new_node.parent = parent_node
                parent_node.children.append(new_node)
                if isinstance(value, (dict, list)):
                    self.build_from_dict(value, new_node)
                else:
                    new_node.custom_properties['value'] = value
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self.build_from_dict(item, parent_node)
                else:
                    child = self.create_node(name=str(item))
                    child.parent = parent_node
                    parent_node.children.append(child)

    def update_database_table_fields(self):
        """
        Recursively traverses the tree to set the 'database_table' property 
        on every node based on its containing branch and configuration state.
        """
        # 1. Identify all branches directly under the root
        # (Assuming your add_yaml logic attaches files as direct children of root)
        for branch in self.root.children:
            
            # We only process nodes that are actually branches (have the hash)
            if 'hash' not in branch.custom_properties:
                continue
                
            # 2. Define a recursive helper to tag the branch and all its descendants
            def tag_node(current_node):
                # Use the function we built to generate the unique name
                table_name = self.get_table_name(current_node, branch)
                
                # Set the field on the node (or within custom_properties)
                current_node.database_table = table_name
                
                # Continue down the tree
                for child in current_node.children:
                    tag_node(child)

            # 3. Start the recursion from this branch down
            tag_node(branch)

    def get_table_name(self, node, branch) -> str:
        """
        Generates a PostGIS-safe table name.
        Uses the node for the name identity and the branch for the data state hash.
        Format: opensite_[short-node-hash]_[full-yml-hash]
        """
        # Generate shortened Name Hash (8 chars) from specific node name
        # This ensures 'Turbine-1' and 'Turbine-2' get different tables
        node_name_clean = str(node.name).strip().lower()
        node_hash = hashlib.md5(node_name_clean.encode()).hexdigest()[:8]

        yml_hash = branch.custom_properties.get('hash')
        
        if not yml_hash:
            raise ValueError(
                f"Branch '{branch.name}' is missing the 'hash' in custom_properties. "
                "Table name cannot be generated without the state fingerprint."
            )

        # Prefixing with 'opensite' ensures it starts with a letter
        table_name = f"opensite_{node_hash}_{yml_hash}"

        return table_name

    def resolve_math(self, expression: Any, context: Dict[str, Any]) -> Any:
        """
        Performs the actual mathematical calculation.
        Example: "1.1 * tip-height" with context {'tip-height': 100} -> 110.0
        """
        # 1. Only process strings. If it's already a number, return it.
        if not isinstance(expression, str):
            return expression

        # 2. Check if any variable names from our context exist in the string.
        # We sort by length descending so 'blade-radius-max' doesn't 
        # get partially replaced by 'blade-radius'.
        sorted_keys = sorted(context.keys(), key=len, reverse=True)
        
        has_variable = False
        templated_expr = expression
        
        for key in sorted_keys:
            if key in templated_expr:
                # Replace the variable name with its actual number
                templated_expr = templated_expr.replace(key, str(context[key]))
                has_variable = True

        # 3. If we found variables, calculate the result.
        if has_variable:
            try:
                # We strip dangerous built-ins to keep eval safe.
                # This performs the actual "math" (multiplication, addition, etc.)
                return eval(templated_expr, {"__builtins__": None}, {})
            except Exception as e:
                # If the math is garbage (e.g. "1.1 * /path/to/osm"), return raw string.
                return expression
        
        return expression
    
    def update_metadata(self, model: dict):
        """Placeholder for updating tree nodes with external metadata."""
        raise NotImplementedError("Subclasses must implement update_metadata")