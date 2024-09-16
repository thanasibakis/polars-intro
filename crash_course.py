# %%
# Load the polars package.
# If you don't have it already, use `pip install polars` in a system shell

import polars as pl

# %%
# Load the data. The `try_parse_dates` argument gives us a date type
# column instead of a string type

data = pl.read_csv("data/counts.csv", try_parse_dates=True)
print(data)

# %%
# Selecting columns. Also note `.drop` removes columns

data.select("division", "lineage")

# %%
# Combining `select` with `unique` is helpful

lineages = data.select("lineage").unique()
print(lineages)
print("This object has type", type(lineages))

# %%
# DataFrame vs Series

# Similar to R, individual columns are a different data structure
# than the whole data frame. Unlike R, though, the data structure
# is not a native type. This is mostly important to know when you
# are looking for a function in the documentation.

# To access a column (like df$col in R), use this notation:

print(data["division"])
print("This object has type", type(data["division"]))

# %%
# Filtering. The key here is creating a "polars expression" using `pl.col()`.

data.filter(pl.col("count") > 100)

# %%
# There is also a shortcut for checking equality that doesn't require `pl.col()`

data.filter(lineage="other")  # or data.filter(pl.col("lineage") == "other")

# %%
# Boolean logic. To chain logical "and", you can just list conditions with commas.
# You could also use the `&` operator`

data.filter(pl.col("count") > 100, pl.col("lineage").is_in(["21J", "21K"]))

# %%
# Logical "or" uses `|`
data.filter((pl.col("count") > 100) | (pl.col("lineage").is_in(["21J", "21K"])))

# %%
# To make code easier to read, polars expressions can be saved as Python variables
# and passed around (far more easily than in R, I have to admit...)

# This makes no actual connection to the data until we run the `filter`

count_filter = pl.col("count") > 100
lineage_filter = pl.col("lineage").is_in(["21J", "21K"])

data.filter(count_filter | lineage_filter)

# %%
# Notice how `is_in` belongs to the `pl.col()`. What else can we call?
# Check https://docs.pola.rs/api/python/stable/reference/expressions/index.html

# There are also type-specific functions that are grouped up; for example,
# date-type operations are in `pl.col().dt`

data.filter(pl.col("date").dt.month() == 11)

# %%
# Creating columns. The R equivalent of `mutate` is `with_columns`.
# As in R, DataFrames are immutable, so save your result if you want it

data.with_columns(
    month=pl.col("date").dt.month(),
    year=pl.col("date").dt.year(),
)

print(data)

# %%
# Grouping and aggregation.
# The first kind of aggregation reduces the number of rows to one per group,
# like `group_by |> summarize` in R

# Notes:
#  - `group_by` can take column names as strings, or polars expressions
#  - `pl.sum("col")` is a shortcut for `pl.col("col").sum()`
#  - We sort at the end to help prove what this does

data.group_by(
    "lineage",
    "division",
    month=pl.col("date").dt.month(),
).agg(
    count=pl.sum("count")
).sort("lineage", "division", "month")

# %%
# The second kind of aggregation doesn't reduce the number of rows, just adds the
# grouped computation result to a new column. This is like `group_by |> mutate` in R

data.with_columns(
    total_count=pl.sum("count").over(
        "lineage",
        "division",
        pl.col("date").dt.month(),
    )
).sort("lineage", "division", "date")

# %%
# Joining. For fun, let's import data on the time zone of each division.
# I asked ChatGPT to generate this file, so I make no guarantees of correctness

# We also rename the state column to match the division column in the data

timezones = pl.read_csv("data/timezones.csv").rename({"state": "division"})
print(timezones)

# %%
# A "left join" keeps all rows in the left table, and fills in data
# from the right table. If a division is in the left table but not the
# right table, a null value will be filled in

data = data.join(timezones, on="division", how="left")
print(data)

# %%
# Null checking

data.filter(pl.col("timezone").is_null())

# %%
# We can replace the null values

data.filter(pl.col("timezone").is_null()).fill_null("Unknown")
