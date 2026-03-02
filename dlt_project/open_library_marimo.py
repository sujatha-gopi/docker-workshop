import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Top 10 Authors by Book Count

    This notebook uses **Marimo** to access a dataset produced by the `open_library_pipeline` and **Ibis** to query the data. We aggregate the number of books per author and visualize the top ten with a bar chart.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Install Dependencies

    Install required packages (if not already available) and import them. We'll use `ibis`, `pandas`, and `matplotlib`. The Marimo client is available via the `dlt` package.
    """)
    return


@app.cell
def _():
    # pip install ibis-framework pandas matplotlib
    # may need to install a marimo-specific package if you want the interactive

    import ibis
    import pandas as pd
    import matplotlib.pyplot as plt

    print("imports successful")
    return ibis, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Connect to Dataset with Ibis

    Use Ibis to open the DuckDB file produced by the pipeline and access the underlying tables.
    """)
    return


@app.cell
def _(ibis):
    # create a DuckDB connection via ibis

    db_path = "open_library_pipeline.duckdb"
    con = ibis.duckdb.connect(db_path)

    # list available tables and select the first one
    tables = con.list_tables()
    print("tables:", tables)

    if not tables:
        raise ValueError("No tables found in the DuckDB file. Make sure the pipeline has run and generated data.")

    # proceed with `con` usage
    return con, tables


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Query for Books and Authors

    Define Ibis table references and inspect the schema. The `books` table (or similar) contains author information.
    """)
    return


@app.cell
def _(con, tables):
    # use the actual table name reported above
    # the dataset may include a schema prefix
    table_name = tables[0]
    print("using table", table_name)
    books = con.table(table_name)

    # show a few rows to identify author column
    books.limit(5).execute()

    # assume the author column is named 'author_name' or similar
    # if multiple authors, adjust accordingly
    return (books,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aggregate Top 10 Authors by Book Count

    Group by the author field, count books, order descending, and limit to ten. Execute the expression and convert to a pandas DataFrame.
    """)
    return


@app.cell
def _(books, ibis):
    author_col = "author_name"  # adjust if different
    expr = (
        books
        .group_by(author_col)
        .aggregate(book_count=books.id.count())
        .order_by(ibis.desc("book_count"))
        .limit(10)
    )

    top10_df = expr.execute()
    top10_df
    return author_col, top10_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Visualize with Matplotlib/Seaborn

    Plot the resulting DataFrame using a bar chart.
    """)
    return


@app.cell
def _(author_col, plt, top10_df):
    # using matplotlib for simplicity
    plt.figure(figsize=(10,6))
    plt.bar(top10_df[author_col], top10_df["book_count"], color="skyblue")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Number of Books")
    plt.title("Top 10 Authors by Book Count")
    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    app.run()
