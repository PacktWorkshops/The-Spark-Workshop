val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()

val example_data = List(
 ("Stephanie", "Smith", "777 Brockton Avenue Abington, MA 2351", "(340) 977-9288")
 , ("Ike", "Dodge", "8478 Mill Pond Rd. Desoto, TX 75115", "(545) 236-9396")
 , ("Lacie", "Culbreath", null, "(782) 339-8539")
 , ("Daniel", "Towles", "45 Water Street Cartersville, GA 30120", "(699) 791-0320")
 , ("Sheena", null, "399 Pierce Street La Vergne, TN 37086", null)
 , ("Joe", "Vigna", "3 Armstrong Street Malvern, PA 19355",null)
 , ("Daniel", "Towles", "45 Water Street Cartersville, GA 30120", "(699) 791-0320")
 , ("Stephanie", "Smith", "500 East Main St. Carlsbad, NM 54986", "(897) 455-1312")
 , ("Gary", "Cliff", "", "")
 , ("Jasmine", "French", "76 S. Lafayette Ave. Cupertino, CA 95014", "(385) 456-9823")
)

val df = spark.createDataFrame(example_data).toDF("first_name", "last_name", "address", "phone_number")
df.show(20, false)


val removed_dups = df.dropDuplicates()
removed_dups.show()

df.dropDuplicates(("first_name", "last_name")).show()

df.dropDuplicates("first_name", "last_name").show()


df.na.drop().show()
df.na.drop(how = "any").show()

df.na.drop(how = "all").show()

df.na.drop(how = "any", cols = ["address", "phone_number"]).show()

df.na.drop(minNonNulls = 3).show()


df.na.fill("This was a null value!").show()

df.na.fill("(555) 555-5555", Array("phone_number")).show()

df.na.fill(5.5).show()


df.na.replace("last_name", Map("French" -> "Davidson")).show()

df.na.replace(Array("address", "phone_number"), Map("" -> "Blank value")).show()

df.na.replace("*", Map("" -> "Blank value")).show()


val number_columns = Array("age", "salary")

val df1 = employee_df.na.fill(employee_df.groupBy().avg().collect()(0)(0).asInstanceOf[Double], Array(number_columns(0)))

val df2 = df1.na.fill(df1.groupBy().avg().collect()(0)(1).asInstanceOf[Double], Array(number_columns(1)))

val number_columns = Array("age", "salary")

val df1 = employee_df.na.fill(employee_df.groupBy().avg().collect()(0)(0).asInstanceOf[Double], Array(number_columns(0)))
val df2 = df1.na.fill(df1.groupBy().avg().collect()(0)(1).asInstanceOf[Double], Array(number_columns(1)))

df2.show()
