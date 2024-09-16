# Our goal is to compute lineage prevalence proportions in each US state
# (+ DC + Puerto Rico) for each month in `data/counts.csv`.
#
# The output should have the following columns:
# 	`division`, `year`, `month`, `lineage`, `proportion`
#
# Provide exactly one row for each possible division-year-month-lineage combination.
# If a lineage is not present in a division-year-month, the proportion should be 0.
# If no lineages are present in a division-year-month, the proportions should be null
# (which is `None` in Python).
#
# Write the result to a CSV called `result.csv`

# %%
import polars as pl

# Load the data, extract the year and month, and sum up counts
# for each division-year-month-lineage combination

# %%
data = (
    pl.read_csv("data/counts.csv", __________________)
    .with_columns(
        year=__________________,
        month=__________________,
    )
    .group_by(__________________)
    .agg(__________________)
)

# To have one row for each possible division-year-month-lineage combination,
# we need to compute the set of all combinations and join it to the data

# %%
key = data.select("division").unique()

for col in ("year", "month", "lineage"):
    key = key.join(__________________, how="cross")

# %%
data = __________________.join(
    __________________,
    how=__________________,
    on=("division", "year", "month", "lineage"),
).fill_null(__________________)

# %%
result = (
    data.with_columns(
        total_count=__________________,
    )
    .with_columns(
        proportion=__________________,
    )
    .fill_nan(None)
)

# %%
result.write_csv("result.csv")
