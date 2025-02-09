long_values = df[df.astype(str).applymap(len).gt(30)]
print(long_values)
