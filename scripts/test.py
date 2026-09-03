import pandas as pd
df = pd.read_parquet("data/fundamentals/fundamentals.parquet")
x = df[df.ticker == "XOM"]
print(x[["metric","period_start","period_end","value","form","filed"]].sort_values("period_end").to_string())