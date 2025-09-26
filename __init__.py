from .parser.sql_parser import SQLParser
from .optimizer.heuristic_optimizer import HeuristicOptimizer
from .optimizer.cost_based_optimizer import CostBasedOptimizer
from .core.logical_plan import LogicalPlan

class QueryOptimizer:
    def __init__(self, statistics_provider):
        self.parser = SQLParser()
        self.heuristic_optimizer = HeuristicOptimizer()
        self.cost_based_optimizer = CostBasedOptimizer(statistics_provider)
        self.statistics_provider = statistics_provider
    
    def optimize(self, sql: str) -> LogicalPlan:
        """Complete optimization pipeline"""
        # 1. Parse SQL to logical plan
        logical_plan = self.parser.parse(sql)
        
        # 2. Apply heuristic optimizations
        heuristic_plan = self.heuristic_optimizer.optimize(logical_plan)
        
        # 3. Apply cost-based optimizations
        final_plan = self.cost_based_optimizer.optimize(heuristic_plan)
        
        return LogicalPlan(final_plan)
    
    def explain(self, sql: str, visualize: bool = True) -> dict:
        """Generate detailed explanation of optimization process"""
        plan = self.optimize(sql)
        
        explanation = {
            'original_sql': sql,
            'optimized_plan': plan.root.to_dict(),
            'total_estimated_cost': plan.root.estimated_cost,
            'total_estimated_rows': plan.root.estimated_rows,
            'heuristic_optimizations': self.heuristic_optimizer.optimization_stats
        }
        
        if visualize:
            image_path = plan.visualize()
            explanation['visualization'] = image_path
            
        return explanation