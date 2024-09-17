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

import polars as pl

# Load the data, extract the year and month, and sum up counts
# for each division-year-month-lineage combination

data = (
    pl.read_csv("data/counts.csv", try_parse_dates=True)
    .with_columns(
        year=pl.col("date").dt.year(),
        month=pl.col("date").dt.month(),
    )
    .group_by("division", "year", "month", "lineage")
    .agg(pl.sum("count"))
)

# To have one row for each possible division-year-month-lineage combination,
# we need to compute the set of all combinations and join it to the data

key = pl.DataFrame({"month": range(1, 13)}, schema={"month": data["month"].dtype})

for col in ("year", "division", "lineage"):
    key = key.join(data.select(col).unique(), how="cross")

data = key.join(
    data,
    how="left",
    on=("division", "year", "month", "lineage"),
).fill_null(0)

result = (
    data.with_columns(
        total_count=pl.sum("count").over("division", "year", "month"),
    )
    .with_columns(
        proportion=pl.col("count") / pl.col("total_count"),
    )
    .fill_nan(None)
    .drop("count", "total_count")
)

result.write_csv("result.csv")
