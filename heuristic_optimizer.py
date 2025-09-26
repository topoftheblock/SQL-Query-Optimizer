from typing import Set, List
import re
from ..core.logical_plan import LogicalNode, NodeType

class HeuristicOptimizer:
    def __init__(self):
        self.rules = [
            self.push_filters_down,
            self.eliminate_redundant_filters,
            self.merge_filters,
            self.reorder_joins,
            self.push_projects_down,
            self.eliminate_redundant_projects
        ]
        self.optimization_stats = {}
    
    def optimize(self, plan: LogicalNode) -> LogicalNode:
        """Apply all heuristic optimization rules"""
        self.optimization_stats = {'rules_applied': 0}
        
        optimized_plan = plan
        max_iterations = 10
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            new_plan = self._apply_rules(optimized_plan)
            
            if self._plans_equal(optimized_plan, new_plan):
                break  # No more changes
                
            optimized_plan = new_plan
        
        self.optimization_stats['iterations'] = iteration
        return optimized_plan
    
    def _apply_rules(self, node: LogicalNode) -> LogicalNode:
        """Apply optimization rules recursively"""
        # Optimize children first
        new_children = [self._apply_rules(child) for child in node.children]
        new_node = LogicalNode(node.node_type, new_children, node.properties.copy())
        new_node.estimated_cost = node.estimated_cost
        new_node.estimated_rows = node.estimated_rows
        
        # Apply rules to this node
        for rule in self.rules:
            result = rule(new_node)
            if result != new_node:
                self.optimization_stats['rules_applied'] += 1
                return result
                
        return new_node
    
    def push_filters_down(self, node: LogicalNode) -> LogicalNode:
        """Push filter operations closer to data source"""
        if node.node_type != NodeType.FILTER:
            return node
            
        if not node.children:
            return node
            
        child = node.children[0]
        condition = node.properties.get("condition", "")
        
        if child.node_type == NodeType.JOIN:
            return self._push_filter_through_join(node, child, condition)
        elif child.node_type == NodeType.PROJECT:
            return self._push_filter_through_project(node, child, condition)
        elif child.node_type in [NodeType.SCAN, NodeType.AGGREGATE]:
            # Filter is already close to source
            return node
            
        return node
    
    def _push_filter_through_join(self, filter_node: LogicalNode, join_node: LogicalNode, condition: str) -> LogicalNode:
        """Push filter down through join"""
        left, right = join_node.children[0], join_node.children[1]
        
        left_columns = self._extract_table_columns(condition, left)
        right_columns = self._extract_table_columns(condition, right)
        
        if left_columns and not right_columns:
            # Filter applies only to left side
            new_left = LogicalNode(NodeType.FILTER, 
                                 children=[left],
                                 properties={"condition": condition})
            return LogicalNode(NodeType.JOIN,
                             children=[new_left, right],
                             properties=join_node.properties)
        elif right_columns and not left_columns:
            # Filter applies only to right side
            new_right = LogicalNode(NodeType.FILTER,
                                  children=[right],
                                  properties={"condition": condition})
            return LogicalNode(NodeType.JOIN,
                             children=[left, new_right],
                             properties=join_node.properties)
        
        return filter_node
    
    def reorder_joins(self, node: LogicalNode) -> LogicalNode:
        """Reorder joins to put smaller tables first"""
        if node.node_type == NodeType.JOIN and len(node.children) == 2:
            left, right = node.children[0], node.children[1]
            
            # Use estimated row counts for reordering
            if left.estimated_rows > right.estimated_rows * 10:  # Significant size difference
                # Swap if left is much larger than right
                return LogicalNode(NodeType.JOIN,
                                 children=[right, left],
                                 properties=node.properties)
        
        return node
    
    def _extract_table_columns(self, condition: str, node: LogicalNode) -> Set[str]:
        """Extract columns referenced in condition that belong to a specific table"""
        table_name = node.properties.get("table", "")
        if not table_name:
            return set()
            
        # Extract all column references from condition
        columns = set()
        words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', condition)
        
        for word in words:
            if word.upper() in ['AND', 'OR', 'NOT', 'LIKE', 'IN', 'BETWEEN', 'NULL']:
                continue
            # Simple heuristic: if word matches a potential column name
            if word.isalpha() and not word.isupper():
                columns.add(word)
                
        return columns
    
    def _plans_equal(self, plan1: LogicalNode, plan2: LogicalNode) -> bool:
        """Check if two plans are equivalent (simplified)"""
        if plan1.node_type != plan2.node_type:
            return False
        if plan1.properties != plan2.properties:
            return False
        if len(plan1.children) != len(plan2.children):
            return False
            
        return all(self._plans_equal(c1, c2) for c1, c2 in zip(plan1.children, plan2.children))