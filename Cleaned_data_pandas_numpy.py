# cleaning text file
import pandas as pd
import numpy as np
import datetime as dt

file_path = r"C:\Users\cheit\OneDrive\Desktop\Mini Python Projects\Coffe_sales.xlsx"

df = pd.read_excel(file_path)

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace("R","£")
)

def dedup_columns(columns):
    seen = {}
    new_cols = []
    for col in columns:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)

        else:
            seen[col] =+ 1
            new_cols.append(f"{col}_{seen[col]}")
    return new_cols

df.columns = dedup_columns(df.columns)

df["datetime"] = pd.to_datetime(df["datetime"])
df["date"] = df["datetime"].dt.date
df["time"] = df["datetime"].dt.time

df.drop(columns=["datetime"], inplace=True)

final_columns = [
    "date",
    "time",
    "hour_of_day",
    "cash_type",
    "card",
    "money",
    "coffee_name",
    "time_of_day",
    "weekday",
    "month_name",
    "weekdaysort",
    "monthsort"
]

df = df.reindex(columns=final_columns)

df.to_csv(
    "Coffee_sales_clean.csv",
    index=False,
    encoding="utf-8",
    quoting=1
)


