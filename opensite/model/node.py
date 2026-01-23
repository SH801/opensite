# Node has:

# URN: Unique numeric ID
# Name, eg "Areas of Outstanding Natural Beauty - Scotland"
# Type: source, destination, null [source will be link on open data portal, destination would be consolidated output file or intermediary processed file, null is for groups]
# Location: None, URL or file 
# Mimetype: None or Type of file - crucial for determining appropriate downloader 
# DatabaseTable: PostGIS database table that stores data for this node
# DatabaseAction: None, buffer, simplify, grid, amalgamate, invert [works on all nodes listed in Dependencies]
# Style: {'fill', 'line'}. Can be null
# CustomProperties: for example if Type=null (ie. group), we might have {'height-to-tip': 105, 'blade-radius': 35, 'notes': 'Created on 01-01-2026 13:01'}
# Status: Unprocessed, Processed
# Dependencies: array of URN dependencies - only when all dependencies are processed can this item be processed
# Log: Array of dict log entries, eg [{'action': 'downloaded', 'time': '2026-01-22 13:00:01'}] 


from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import time

@dataclass
class Node:
    urn: int
    name: str
    title: Optional[str] = None
    node_type: Optional[str] = None  # 'source', 'destination', or None (Group)
    url: Optional[str] = None
    format: Optional[str] = None
    database_table: Optional[str] = None
    database_action: Optional[str] = None  # buffer, simplify, grid, amalgamate, invert
    style: Optional[Dict[str, Any]] = None
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    status: str = "unprocessed"
    
    @property
    def dependencies(self) -> List[int]:
        """Automatically returns a list of URNs for all direct children."""
        return [child.urn for child in self.children]

    log: List[Dict[str, Any]] = field(default_factory=list)
    
    # Hierarchy
    parent: Optional['Node'] = None
    children: List['Node'] = field(default_factory=list)

    def add_log(self, action: str):
        self.log.append({
            'action': action,
            'time': time.strftime('%Y-%m-%d %H:%M:%S')
        })

    def get_property(self, key: str) -> Any:
        """Recursive lookup: returns property from self or nearest ancestor."""
        if key in self.custom_properties:
            return self.custom_properties[key]
        if self.parent:
            return self.parent.get_property(key)
        return None
    
    def to_json(self) -> Dict[str, Any]:
        """Converts the node and its subtree into a JSON-serializable dictionary."""
        return {
            "urn": self.urn,
            "name": self.name,
            "node_type": self.node_type,
            "location": self.location,
            "format": self.format,
            "database_table": self.database_table,
            "database_action": self.database_action,
            "style": self.style,
            "custom_properties": self.custom_properties,
            "status": self.status,
            "dependencies": self.dependencies,
            "log": self.log,
            "children": [child.to_json() for child in self.children]
        }