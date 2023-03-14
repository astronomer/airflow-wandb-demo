FROM quay.io/astronomer/astro-runtime:7.3.0

#Installing in venv due to pyarrow dependency issues.
#https://github.com/astronomer/astro-sdk/blob/08b73675bd6855e11bc9bcbfd089a0ba4536c52d/python-sdk/pyproject.toml#L67
#https://github.com/dbt-labs/dbt-snowflake/blob/003d8946e10c9aafdf1517b645a73652a0fabe0c/setup.py#L71
RUN python -m virtualenv /home/astro/.venv/dbt && \
    source /home/astro/.venv/dbt/bin/activate && \
    pip install --no-cache-dir dbt-core==1.4.5 dbt-snowflake==1.4.1 && \
    deactivate