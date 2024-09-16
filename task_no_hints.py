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
