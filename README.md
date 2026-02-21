## Data Warehouse Modeling

The curated dataset is further modeled into a star schema to support analytical workloads.

### Fact Table
**fact_sales**
- transaction_id (Degenerate Dimension)
- date_id
- product_id
- store_id
- quantity
- total_amount
- profit

### Dimension Tables

**dim_product**
- product_id
- product_name
- category
- brand
- cost_price

**dim_store**
- store_id
- store_name
- city
- region

**dim_date**
- date_id
- full_date
- day
- month
- year
- quarter
- day_of_week

### Note on Degenerate Dimension
`transaction_id` is modeled as a degenerate dimension since it uniquely identifies a transaction but has no descriptive attributes.


v1

Basic PySpark ETL
Medallion architecture

v2
Structured logging
Incremental loading
Dimensional modeling (Star schema)
Data warehouse layer
Observability metrics
