import re
from typing import Dict, List, Any
from ..core.logical_plan import LogicalNode, NodeType

class SQLParser:
    def __init__(self):
        self.aliases = {}
        
    def parse(self, sql: str) -> LogicalNode:
        """Parse SQL query into logical plan"""
        sql = self._normalize_sql(sql)
        return self._parse_select(sql)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for easier parsing"""
        # Remove extra whitespace and convert to uppercase
        sql = re.sub(r'\s+', ' ', sql).strip().upper()
        return sql
    
    def _parse_select(self, sql: str) -> LogicalNode:
        """Parse SELECT statement"""
        # Extract clauses using more robust regex
        select_match = re.search(r'SELECT(.+?)FROM', sql, re.IGNORECASE | re.DOTALL)
        from_match = re.search(r'FROM(.+?)(WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        where_match = re.search(r'WHERE(.+?)(GROUP BY|HAVING|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        join_match = re.search(r'((INNER|LEFT|RIGHT|FULL) JOIN)(.+?)ON', sql, re.IGNORECASE | re.DOTALL)
        group_match = re.search(r'GROUP BY(.+?)(HAVING|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        order_match = re.search(r'ORDER BY(.+?)(LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        
        # Build plan from bottom up
        current_node = self._parse_from_clause(from_match.group(1) if from_match else "")
        
        # Handle JOINs
        if join_match:
            current_node = self._parse_join(current_node, join_match)
            
        # Handle WHERE
        if where_match:
            current_node = LogicalNode(NodeType.FILTER, 
                                     children=[current_node],
                                     properties={"condition": where_match.group(1).strip()})
        
        # Handle GROUP BY
        if group_match:
            current_node = LogicalNode(NodeType.AGGREGATE,
                                     children=[current_node],
                                     properties={"group_by": group_match.group(1).strip()})
        
        # Handle ORDER BY  
        if order_match:
            current_node = LogicalNode(NodeType.SORT,
                                     children=[current_node],
                                     properties={"order_by": order_match.group(1).strip()})
        
        # Handle LIMIT
        if limit_match:
            current_node = LogicalNode(NodeType.LIMIT,
                                     children=[current_node],
                                     properties={"limit": int(limit_match.group(1))})
        
        # Handle SELECT (projection)
        if select_match:
            columns = [col.strip() for col in select_match.group(1).split(',')]
            current_node = LogicalNode(NodeType.PROJECT,
                                     children=[current_node],
                                     properties={"columns": columns})
        
        return current_node
    
    def _parse_from_clause(self, from_clause: str) -> LogicalNode:
        """Parse FROM clause"""
        tables = [t.strip() for t in from_clause.split(',')]
        
        if len(tables) == 1:
            table = tables[0]
            # Handle table aliases
            if ' AS ' in table:
                table, alias = table.split(' AS ')
                self.aliases[alias.strip()] = table.strip()
            return LogicalNode(NodeType.SCAN, properties={"table": table.strip()})
        else:
            # Multiple tables - start with cartesian product
            scan_nodes = [LogicalNode(NodeType.SCAN, properties={"table": t.strip()}) for t in tables]
            current_node = scan_nodes[0]
            for node in scan_nodes[1:]:
                current_node = LogicalNode(NodeType.JOIN, children=[current_node, node])
            return current_node
    
    def _parse_join(self, current_node: LogicalNode, join_match) -> LogicalNode:
        """Parse JOIN clause"""
        join_type = join_match.group(1).upper()
        join_condition = sql.split('ON', 1)[1].split('WHERE')[0].split('GROUP BY')[0].strip()
        
        # For simplicity, assume the right table is in the JOIN clause
        join_tables = re.findall(r'(\w+)\s+(?:AS\s+)?\w*\s+JOIN', join_match.group(0))
        
        if join_tables:
            right_table = join_tables[-1]
            right_node = LogicalNode(NodeType.SCAN, properties={"table": right_table})
            
            return LogicalNode(NodeType.JOIN,
                             children=[current_node, right_node],
                             properties={"type": join_type, "condition": join_condition})
        
        return current_node