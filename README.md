# Text-to-SQL Neo4j Knowledge Graph Builder

This project reads YAML metadata files and builds a Neo4j knowledge graph for visualization and Cypher querying.

## Project Structure

```
text2sql/
├── build_neo4j_graph.py          # Main script to build the Neo4j graph
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── metadata/
    └── domains/
        └── vnfilm_ticketing/     # Domain folder
            ├── tables/           # Table YAML files
            │   ├── orders.yaml
            │   ├── vendor.yaml
            │   └── bank.yaml
            ├── joins.yaml        # Join definitions
            └── metrics.yaml      # Business metrics
```

## Prerequisites

1. **Python 3.10+** installed
2. **Neo4j Database** running (local or remote)
   - Download Neo4j Desktop: https://neo4j.com/download/
   - Or use Neo4j Aura (cloud): https://neo4j.com/cloud/aura/

## Installation

```bash
# Create virtual environment (optional but recommended)
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Set the following environment variables before running:

```bash
# Windows CMD
set NEO4J_URI=bolt://localhost:7687
set NEO4J_USER=neo4j
set NEO4J_PASSWORD=your_password

# Windows PowerShell
$env:NEO4J_URI = "bolt://localhost:7687"
$env:NEO4J_USER = "neo4j"
$env:NEO4J_PASSWORD = "your_password"

# Linux/Mac
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=your_password
```

## Usage

```bash
# Run with default settings (domain: vnfilm_ticketing, metadata-root: metadata/domains)
python build_neo4j_graph.py

# Specify a different domain
python build_neo4j_graph.py --domain vnfilm_ticketing

# Specify a different metadata root
python build_neo4j_graph.py --metadata-root metadata/domains --domain vnfilm_ticketing
```

## Neo4j Graph Model

### Node Labels

| Label     | Key Properties                      | Description                          |
|-----------|-------------------------------------|--------------------------------------|
| `:Table`  | `domain`, `table_name`              | Database tables (fact/dimension)     |
| `:Column` | `table_name`, `column_name`         | Table columns with metadata          |
| `:Concept`| `name`                              | Business concepts with synonyms      |
| `:Metric` | `name`                              | Business metrics with expressions    |

### Relationships

| Relationship          | From       | To         | Properties                              |
|-----------------------|------------|------------|----------------------------------------|
| `HAS_COLUMN`          | Table      | Column     | `primary_key`, `time_column`           |
| `JOIN`                | Table      | Table      | `join_type`, `on`, `description`       |
| `FK`                  | Table      | Table      | `column`, `references_column`, etc.    |
| `HAS_CONCEPT`         | Table      | Concept    | `source`                               |
| `HAS_SEMANTIC`        | Column     | Concept    | (matches column semantics to concepts) |
| `METRIC_BASE_TABLE`   | Metric     | Table      | (links metric to its base table)       |
| `HAS_METRIC`          | Table      | Metric     | (reverse link from table to metric)    |

## Example Cypher Queries

After running the script, open Neo4j Browser and try these queries:

### View all tables and their joins
```cypher
MATCH (t:Table)-[r:JOIN]->(t2:Table)
RETURN t, r, t2
LIMIT 50
```

### View tables with their columns
```cypher
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN t, c
LIMIT 100
```

### View metrics and their base tables
```cypher
MATCH (m:Metric)-[:METRIC_BASE_TABLE]->(t:Table)
RETURN m, t
```

### Find all concepts related to a table
```cypher
MATCH (t:Table {table_name: 'orders'})-[:HAS_CONCEPT]->(c:Concept)
RETURN t.table_name, c.name, c.synonyms
```

### Find columns with specific semantics
```cypher
MATCH (col:Column)-[:HAS_SEMANTIC]->(c:Concept {name: 'money'})
RETURN col.table_name, col.column_name, col.description
```

### View the full schema for a domain
```cypher
MATCH (t:Table {domain: 'vnfilm_ticketing'})
OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
OPTIONAL MATCH (t)-[:JOIN]->(t2:Table)
OPTIONAL MATCH (t)-[:HAS_METRIC]->(m:Metric)
RETURN t, c, t2, m
```

### Find foreign key relationships
```cypher
MATCH (t1:Table)-[fk:FK]->(t2:Table)
RETURN t1.table_name, fk.column, t2.table_name, fk.references_column
```

### Search for tables/columns by business concept
```cypher
// Find all tables related to "promotion"
MATCH (t:Table)-[:HAS_CONCEPT]->(c:Concept)
WHERE c.name = 'promotion' OR 'promotion' IN c.synonyms
RETURN DISTINCT t.table_name, t.business_name
```

## Adding New Tables

1. Create a new YAML file in `metadata/domains/<domain>/tables/`:

```yaml
catalog: lakehouse
schema: lh_vnfilm_v2
table_name: new_table
domain: vnfilm_ticketing
table_type: fact  # or dim
business_name: "Human readable name"
grain: "1 row = 1 ..."
description: "Description of the table"
tags: [tag1, tag2]
primary_key: [id]
foreign_keys:
  - column: other_table_id
    references_table: "catalog.schema.other_table"
    references_column: "id"
    relation: "many_to_one"
    description: "Join description"
time_columns: [created_date]
concepts:
  - name: concept_name
    synonyms: ["synonym1", "synonym2"]
columns:
  id:
    data_type: bigint
    business_name: "ID"
    description: "Primary key"
    semantics: [id, primary]
```

2. Update `joins.yaml` if the new table has joins with existing tables.

3. Update `metrics.yaml` if there are new metrics based on the table.

4. Re-run the script to update the graph.

## Troubleshooting

### Connection Refused
- Make sure Neo4j is running
- Check the URI (default: `bolt://localhost:7687`)
- Verify username and password

### Authentication Failed
- Check NEO4J_USER and NEO4J_PASSWORD environment variables
- Default Neo4j user is `neo4j`

### Constraint Errors
- The script creates constraints on first run
- If you see constraint errors, they may already exist (this is OK)

## License

MIT License
