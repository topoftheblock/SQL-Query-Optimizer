# SQL Query Optimizer

*A sophisticated SQL query optimizer implementing cost-based and heuristic optimization techniques used in modern database systems.*

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🚀 Features

- **Heuristic Optimization**: Predicate pushdown, join reordering, filter merging
- **Cost-Based Optimization**: Statistical analysis and cost estimation
- **Multiple Join Algorithms**: Nested loop, hash join, merge join support
- **Visual Query Plans**: Automatic visualization of optimization steps
- **Comprehensive SQL Support**: `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`

---

## 📋 Quick Start

### Installation

```bash
pip install sql-query-optimizer
Usage
 Copyfrom sql_query_optimizer import QueryOptimizer
from sql_query_optimizer.core import StatisticsProvider

# Create statistics for your tables
stats_provider = StatisticsProvider()
stats_provider.add_table_stats('users', row_count=10000, distinct_count=5000)
stats_provider.add_table_stats('orders', row_count=50000, distinct_count=10000)

# Initialize optimizer
optimizer = QueryOptimizer(stats_provider)

# Optimize a SQL query
sql = """
SELECT users.name, orders.total
FROM users
JOIN orders ON users.id = orders.user_id
WHERE users.age > 30 AND orders.amount > 100
"""

result = optimizer.optimize(sql)
print(f"Optimized plan cost: {result.root.estimated_cost:.2f}")

🔍 Optimization Pipeline

SQL Query → Parser → Logical Plan
Logical Plan → Heuristic Optimizer
Heuristic Optimizer → Cost-Based Optimizer
Cost-Based Optimizer → Physical Plan


📊 Example: Query Optimization
 Copysql = "SELECT * FROM users WHERE age > 25 AND country = 'USA'"
plan = optimizer.optimize(sql)
print(plan.root)
Advanced Example
 Copysql = """
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 30 AND o.amount > 50
"""
explanation = optimizer.explain(sql)
print(f"Cost improvement: {explanation['improvement_percent']}%")

📄 License
This project is licensed under the MIT License – see the LICENSE file for details.
