# Netflix dbt Project

dbt project for transforming MovieLens data in Snowflake into a dimensional model.

## Data flow

- **Raw** (Snowflake) → **Staging** (`src_*` models) → **DWH** (dim, fct, mart)

## Quick start

```bash
dbt run
dbt test
```

Run a single model: `dbt run --select <model_name>`

## Resources

- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Snowflake profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
