# Netflix Data Analysis

A dbt-based analytics project that transforms [MovieLens](https://grouplens.org/datasets/movielens/) data into a dimensional model for analysis. Built for **Snowflake** with staging, dimension, fact, and mart layers.

## Tech stack

- **dbt Core** – transformations and modeling  
- **Snowflake** – warehouse  
- **uv** – Python/dependency management  
- **Data**: MovieLens (movies, ratings, tags, genome scores)

## Project structure

```
netflix_data_analysis/
├── netflix/                 # dbt project
│   ├── models/
│   │   ├── staging/         # Raw → cleaned (src_*)
│   │   ├── dim/             # Dimension tables
│   │   ├── fct/             # Fact tables
│   │   └── mart/            # Analytics-ready marts
│   ├── seeds/               # Reference data (e.g. release dates)
│   ├── snapshots/           # SCD-type tracking
│   ├── macros/              # Reusable SQL
│   └── tests/               # Data quality tests
├── pyproject.toml           # Python + dbt deps (uv)
└── README.md
```

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Snowflake account
- MovieLens data loaded into Snowflake (e.g. from S3 or another source)

## Setup

1. **Clone and install dependencies**

   ```bash
   git clone https://github.com/YOUR_USERNAME/netflix_data_analysis.git
   cd netflix_data_analysis
   uv sync
   ```

2. **Configure dbt**

   Create `~/.dbt/profiles.yml` (or `netflix/profiles.yml` and ensure it is not committed) with your Snowflake connection. See [dbt Snowflake setup](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile).

3. **Run dbt**

   ```bash
   cd netflix
   dbt run
   dbt test
   ```

## Usage

- Run all models: `dbt run`
- Run a specific model: `dbt run --select <model_name>`
- Run tests: `dbt test`
- Build docs: `dbt docs generate && dbt docs serve`

## License

MIT (or your preferred license)
