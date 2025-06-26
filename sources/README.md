# Source Modules

This package contains modules responsible for fetching raw datasets used by the data warehouse. Each module exposes a single `fetch()` function which downloads data and writes it to the appropriate location under `dbt/seeds/`.

To add a new source:

1. Create a new Python module inside `sources/`.
2. Implement a `fetch()` function that performs the data download and persists the results.
3. Reference the function in `pipeline_config.yml` (for example `sources.my_source.fetch`).

Modules can also be executed directly for ad-hoc runs using:

```bash
python -m sources.my_source
```
