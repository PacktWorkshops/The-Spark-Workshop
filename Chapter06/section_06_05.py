spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()


example_data = [
  ["Stephanie", "Smith", "777 Brockton Avenue Abington, MA 2351", "(340) 977-9288"]
  , ["Ike", "Dodge", "8478 Mill Pond Rd. Desoto, TX 75115", "(545) 236-9396"]
  , ["Lacie", "Culbreath", None, "(782) 339-8539"]
  , ["Daniel", "Towles", "45 Water Street Cartersville, GA 30120", "(699) 791-0320"]
  , ["Sheena", None, "399 Pierce Street La Vergne, TN 37086", None]
  , ["Joe", "Vigna", "3 Armstrong Street Malvern, PA 19355",None]
  , ["Daniel", "Towles", "45 Water Street Cartersville, GA 30120", "(699) 791-0320"]
  , ["Stephanie", "Smith", "500 East Main St. Carlsbad, NM 54986", "(897) 455-1312"]
  , ["Gary", "Cliff", "", ""]
  , ["Jasmine", "French", "76 S. Lafayette Ave. Cupertino, CA 95014", "(385) 456-9823"]
]

df = spark.createDataFrame(example_data, ["first_name", "last_name", "address", "phone_number"])
df.show(20, False)


removed_dups = df.dropDuplicates()
removed_dups.show()


df.dropDuplicates(["first_name", "last_name"]).show()

df.dropna().show()


df.dropna().show()
df.dropna(how = "any").show()


df.dropna(how = "all").show()


df.dropna(how = "any", subset = ["first_name", "last_name"]).show()

df.dropna(thresh=3).show()


df.fillna("This was a null value!").show()


df.fillna("(555) 555-5555", ["phone_number"]).show()

df.fillna(5.5).show()

df.replace("French", "Davidson", ["last_name"]).show()

df.replace("", "Blank value").show()



for i in range(0,3):
  print(i)


for index, element in enumerate(["sales", "customer", "product"]):
  print(element + " - " + str(index))


employee_df = spark.createDataFrame(example_data_with_age, ["first_name", "last_name", "address", "phone_number", "age", "salary"])
employee_df.show(20, False)

employee_df.groupBy().avg().collect()

employee_df.groupBy().avg().collect()[0][0]

updated_df = employee_df

for index, element in enumerate(["age", "salary"]):
  updated_df = updated_df.fillna(updated_df.groupBy().avg().collect()[0][index], [element])

updated_df.show()
