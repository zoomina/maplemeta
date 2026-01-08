import pandas as pd

df = pd.read_csv("user_ocid_20250105_200.csv")
df = df.head(100)
df.to_csv("user_ocid_20250105.csv")