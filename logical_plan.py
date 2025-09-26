from typing import List, Dict, Any, Optional, Union
from enum import Enum
import math
from dataclasses import dataclass
from graphviz import Digraph

class NodeType(Enum):
    SCAN = "Scan"
    FILTER = "Filter"
    PROJECT = "Project"
    JOIN = "Join"
    AGGREGATE = "Aggregate"
    SORT = "Sort"
    LIMIT = "Limit"

@dataclass
class Statistics:
    row_count: int = 0
    distinct_count: int = 0
    null_count: int = 0
    min_val: Any = None
    max_val: Any = None
    data_size: int = 0

class LogicalNode:
    def __init__(self, node_type: NodeType, children=None, properties=None):
        self.node_type = node_type
        self.children = children or []
        self.properties = properties or {}
        self.estimated_cost = 0.0
        self.estimated_rows = 0
        self.statistics = Statistics()
        self.id = id(self)  # Unique identifier for visualization
        
    def __str__(self, level=0):
        indent = "  " * level
        cost_info = f"cost={self.estimated_cost:.2f}, rows={self.estimated_rows}"
        props = ", ".join(f"{k}={v}" for k, v in self.properties.items())
        result = f"{indent}{self.node_type.value}({cost_info})"
        if props:
            result += f" [{props}]"
        
        for child in self.children:
            result += "\n" + child.__str__(level + 1)
        return result
    
    def to_dict(self):
        """Convert to dictionary for serialization"""
        return {
            'node_type': self.node_type.value,
            'properties': self.properties,
            'estimated_cost': self.estimated_cost,
            'estimated_rows': self.estimated_rows,
            'children': [child.to_dict() for child in self.children]
        }

class LogicalPlan:
    def __init__(self, root: LogicalNode):
        self.root = root
        self.total_cost = 0.0
        
    def visualize(self, filename: str = "query_plan"):
        """Generate visualization of the query plan"""
        dot = Digraph(comment='Query Execution Plan')
        self._add_nodes(dot, self.root)
        dot.render(filename, format='png', cleanup=True)
        return filename + '.png'
    
    def _add_nodes(self, dot, node: LogicalNode, parent_id: str = None):
        """Recursively add nodes to graphviz diagram"""
        node_id = str(node.id)
        label = f"{node.node_type.value}\\ncost: {node.estimated_cost:.2f}\\nrows: {node.estimated_rows}"
        
        if node.properties:
            props = "\\n".join(f"{k}: {v}" for k, v in node.properties.items())
            label += f"\\n{props}"
        
        dot.node(node_id, label)
        
        if parent_id:
            dot.edge(parent_id, node_id)
            
        for child in node.children:
            self._add_nodes(dot, child, node_id)