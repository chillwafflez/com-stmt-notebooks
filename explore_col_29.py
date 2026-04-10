# ============================================================
# Data exploration for col_29 and _source_file
# Assumes your pandas DataFrame is named `pandas_df`
# ============================================================


# ---- 1. Filter to rows where col_29 has a value ----
df_col29 = pandas_df[pandas_df["col_29"].notna()]
print(df_col29.shape)   # should show (58758, <num_cols>)

# If "empty" means empty strings instead of NaN, use this instead:
# df_col29 = pandas_df[pandas_df["col_29"].astype(str).str.strip() != ""]


# ---- 2. Distinct values and what's in col_29 ----
print("Distinct values in col_29:", df_col29["col_29"].nunique())

# Value counts (what the values are + how many of each)
df_col29["col_29"].value_counts(dropna=False)

# Peek at sample values
df_col29["col_29"].unique()[:20]   # first 20 distinct values
df_col29["col_29"].sample(10)      # 10 random rows' values


# ---- 3. _source_file breakdown for those rows ----
source_counts = df_col29["_source_file"].value_counts(dropna=False)
print(source_counts)

# As a tidy DataFrame with named columns
source_counts_df = (
    df_col29["_source_file"]
    .value_counts(dropna=False)
    .rename_axis("_source_file")
    .reset_index(name="row_count")
)
display(source_counts_df)


# ---- 4. Cross-tab: which col_29 values came from which _source_file ----
# Row counts for each (_source_file, col_29) pair
df_col29.groupby("_source_file")["col_29"].value_counts()

# Distinct col_29 values per source file
df_col29.groupby("_source_file")["col_29"].nunique().sort_values(ascending=False)


# ---- 5. Quick all-in-one summary ----
df_col29 = pandas_df[pandas_df["col_29"].notna()]

print(f"Rows with col_29 populated: {len(df_col29)}")
print(f"Distinct col_29 values:     {df_col29['col_29'].nunique()}")
print(f"Distinct _source_files:     {df_col29['_source_file'].nunique()}")
print(f"col_29 dtype:               {df_col29['col_29'].dtype}")

print("\nTop col_29 values:")
print(df_col29["col_29"].value_counts().head(10))

print("\nRows per _source_file:")
print(df_col29["_source_file"].value_counts())


# ============================================================
# Drill into non-zero col_29 values and their source files
# ============================================================


# ---- Filter out the rows where col_29 is 0 ----
df_nonzero = df_col29[df_col29["col_29"] != 0]

print(f"Rows with non-zero col_29: {len(df_nonzero)}")
print(f"Distinct non-zero values:  {df_nonzero['col_29'].nunique()}")


# ---- Option 1: Grouped view — source files per col_29 value ----
breakdown = (
    df_nonzero
    .groupby("col_29")["_source_file"]
    .value_counts()
    .rename("row_count")
    .reset_index()
    .sort_values(["col_29", "row_count"], ascending=[True, False])
)
display(breakdown)


# ---- Option 2: One row per col_29 value, with source files as a list ----
summary = (
    df_nonzero
    .groupby("col_29")
    .agg(
        total_rows=("_source_file", "size"),
        distinct_source_files=("_source_file", "nunique"),
        source_files=("_source_file", lambda s: sorted(s.unique().tolist())),
    )
    .reset_index()
    .sort_values("total_rows", ascending=False)
)
display(summary)


# ---- Option 3: Inspect one specific value at a time ----
# All rows with a specific col_29 value
df_nonzero[df_nonzero["col_29"] == 23]

# Or just their source files
df_nonzero.loc[df_nonzero["col_29"] == 23, "_source_file"].tolist()


# ---- Option 4: Pivot table — col_29 × _source_file matrix ----
pivot = (
    df_nonzero
    .pivot_table(
        index="col_29",
        columns="_source_file",
        values=df_nonzero.columns[0],   # any column — we just want the count
        aggfunc="size",
        fill_value=0,
    )
)
display(pivot)
