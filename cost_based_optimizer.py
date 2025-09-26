import math
from typing import List, Dict, Tuple
from ..core.logical_plan import LogicalNode, NodeType, Statistics

class CostBasedOptimizer:
    def __init__(self, statistics_provider):
        self.stats_provider = statistics_provider
        self.join_methods = ['nested_loop', 'hash_join', 'merge_join']
        self.cost_cache = {}
        
    def optimize(self, logical_plan: LogicalNode) -> LogicalNode:
        """Generate optimal physical plan using cost-based optimization"""
        plan_with_stats = self._estimate_statistics(logical_plan)
        return self._find_optimal_plan(plan_with_stats)
    
    def _estimate_statistics(self, node: LogicalNode) -> LogicalNode:
        """Estimate statistics for each node in the plan"""
        if node.node_type == NodeType.SCAN:
            table_name = node.properties.get("table")
            if table_name:
                stats = self.stats_provider.get_table_stats(table_name)
                node.statistics = stats
                node.estimated_rows = stats.row_count
                node.estimated_cost = stats.row_count * 0.1  # Base I/O cost
        
        elif node.node_type == NodeType.FILTER:
            child = self._estimate_statistics(node.children[0])
            condition = node.properties.get("condition", "")
            selectivity = self._estimate_selectivity(condition, child)
            
            node.estimated_rows = max(1, int(child.estimated_rows * selectivity))
            node.estimated_cost = child.estimated_cost + node.estimated_rows * 0.01  # CPU cost
            node.children = [child]
        
        elif node.node_type == NodeType.JOIN:
            left = self._estimate_statistics(node.children[0])
            right = self._estimate_statistics(node.children[1])
            
            join_cardinality = self._estimate_join_cardinality(left, right, node)
            join_cost, best_method = self._estimate_join_cost(left, right, node)
            
            node.estimated_rows = join_cardinality
            node.estimated_cost = left.estimated_cost + right.estimated_cost + join_cost
            node.properties['join_method'] = best_method
            node.children = [left, right]
        
        elif node.node_type == NodeType.PROJECT:
            child = self._estimate_statistics(node.children[0])
            node.estimated_rows = child.estimated_rows
            node.estimated_cost = child.estimated_cost + child.estimated_rows * 0.001
            node.children = [child]
            
        return node
    
    def _estimate_selectivity(self, condition: str, node: LogicalNode) -> float:
        """Estimate selectivity of a condition"""
        if not condition:
            return 1.0
            
        # Simple selectivity estimation based on condition type
        conditions = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
        overall_selectivity = 1.0
        
        for cond in conditions:
            if '=' in cond:
                selectivity = 0.1  # Equality condition
            elif any(op in cond for op in ['>', '<', '>=', '<=']):
                selectivity = 0.3  # Range condition
            elif 'LIKE' in cond.upper():
                selectivity = 0.5  # Pattern matching
            else:
                selectivity = 0.8  # Default
                
            overall_selectivity *= selectivity
            
        return max(0.01, overall_selectivity)  # Avoid extremely small selectivities
    
    def _estimate_join_cardinality(self, left: LogicalNode, right: LogicalNode, 
                                 join_node: LogicalNode) -> float:
        """Estimate join cardinality"""
        condition = join_node.properties.get("condition", "")
        
        if not condition or '=' not in condition:
            # Cross join
            return left.estimated_rows * right.estimated_rows
        
        # Equi-join estimation
        left_stats = left.statistics
        right_stats = right.statistics
        
        # Use minimum of distinct counts for equi-join
        min_distinct = min(left_stats.distinct_count, right_stats.distinct_count) or 1
        
        return (left.estimated_rows * right.estimated_rows) / min_distinct
    
    def _estimate_join_cost(self, left: LogicalNode, right: LogicalNode,
                          join_node: LogicalNode) -> Tuple[float, str]:
        """Estimate cost for different join methods"""
        costs = {}
        
        # Nested Loop Join
        costs['nested_loop'] = left.estimated_rows * right.estimated_rows * 0.01
        
        # Hash Join
        costs['hash_join'] = (left.estimated_rows + right.estimated_rows) * 0.1
        
        # Merge Join (requires sorted inputs)
        sort_cost = left.estimated_rows * math.log(left.estimated_rows + 1) * 0.01
        sort_cost += right.estimated_rows * math.log(right.estimated_rows + 1) * 0.01
        costs['merge_join'] = sort_cost + (left.estimated_rows + right.estimated_rows) * 0.05
        
        # Choose best method
        best_method = min(costs, key=costs.get)
        return costs[best_method], best_method
    
    def _find_optimal_plan(self, node: LogicalNode) -> LogicalNode:
        """Explore alternative plans and choose the cheapest"""
        if not node.children:
            return node
            
        # Generate alternative implementations
        alternatives = self._generate_alternatives(node)
        
        # Optimize children first
        optimized_children = [self._find_optimal_plan(child) for child in node.children]
        
        # Evaluate alternatives
        best_plan = node
        best_plan.children = optimized_children
        best_plan = self._estimate_statistics(best_plan)
        best_cost = best_plan.estimated_cost
        
        for alternative in alternatives:
            alternative.children = optimized_children
            alternative = self._estimate_statistics(alternative)
            
            if alternative.estimated_cost < best_cost:
                best_plan = alternative
                best_cost = alternative.estimated_cost
                
        return best_plan
    
    def _generate_alternatives(self, node: LogicalNode) -> List[LogicalNode]:
        """Generate alternative physical implementations"""
        alternatives = []
        
        if node.node_type == NodeType.JOIN:
            # Different join orders
            if len(node.children) == 2:
                swapped = LogicalNode(NodeType.JOIN, 
                                    children=[node.children[1], node.children[0]],
                                    properties=node.properties.copy())
                alternatives.append(swapped)
                
            # Different join methods
            for method in self.join_methods:
                if method != node.properties.get('join_method'):
                    alt_node = LogicalNode(NodeType.JOIN, 
                                         properties=node.properties.copy())
                    alt_node.properties['join_method'] = method
                    alternatives.append(alt_node)
                    
        return alternatives