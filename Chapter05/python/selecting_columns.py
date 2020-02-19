spark = SparkSession \
   .builder \
   .appName("selecting_columns") \
   .getOrCreate()

# Selecting Columns of a DataFrame

df.select("first_name", "last_name").show(5)

df.select("last_name", "first_name").show(5)

df.select("*").show()

df.select(df['first_name'], df['country'], df['salary']).show()

df.select(df.id, df.first_name, df.last_name).show()

df.select(df['first_name'], df.country, df.salary).show()
