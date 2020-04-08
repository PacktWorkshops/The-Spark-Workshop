#!/usr/bin/env python

# create a spark session
import pyspark
spark_context = pyspark.SparkContext()
spark_session = pyspark.sql.SparkSession(spark_context)


# load csv file into dataframe
df = spark_session.read.csv("ab_data.csv", header=True, sep=",")
df.show()


# calculate the conversion rate for the control group
num_control_users = df.filter( df.group == "control").count()
num_control_converted =     df.filter( ( df.group == "control") & ( df.converted == 1) ).count()
print("control group conversion rate = ",     num_control_converted / num_control_users )


# calculate the conversion rate for the treatment group
num_treatment_users = df.filter( df.group == "treatment").count()
num_treatment_converted =     df.filter( ( df.group == "treatment") & ( df.converted == 1) ).count()
print("treatment group conversion rate = ",     num_treatment_converted / num_treatment_users )


# calculate the p-value
import statsmodels.api as sm
_, p_value = sm.stats.proportions_ztest(     [num_control_converted, num_treatment_converted],     [num_control_users, num_treatment_users],     alternative='smaller')
print("A/B test p-value =", p_value)     


# detect any null values
num_rows_with_null_vals = df.filter( df.user_id.isNull() |                                      df.timestamp.isNull() |                                      df.group.isNull() |                                      df.landing_page.isNull() |                                      df.converted.isNull() ).count()
print("Number of rows with null values = ", num_rows_with_null_vals)


#detect non-unique users
total_rows = df.count()
unique_users = df.select("user_id").distinct().count()
print("total rows =", total_rows," unique users =", unique_users)


#drop duplicates
ndf = df.dropDuplicates(["user_id"])
print("total rows =", ndf.count())


# detect any group/landing_page mismatch
df = df.filter( ( ( df.group == "control") &
                  ( df.landing_page == "old_page") ) |
                ( ( df.group == "treatment") &
                  ( df.landing_page == "new_page") ) )
print("new dataframe size =", df.count())


# recalculate the conversion rate for the control group
num_control_users = df.filter( df.group == "control").count()
num_control_converted =     df.filter( ( df.group == "control") & ( df.converted == 1) ).count()
print("control group conversion rate = ",     num_control_converted / num_control_users )


# recalculate the conversion rate for the treatment group
num_treatment_users = df.filter( df.group == "treatment").count()
num_treatment_converted =     df.filter( ( df.group == "treatment") & ( df.converted == 1) ).count()
print("treatment group conversion rate = ",     num_treatment_converted / num_treatment_users )


# recalculate the p-value
import statsmodels.api as sm
_, p_value = sm.stats.proportions_ztest(     [num_control_converted, num_treatment_converted],     [num_control_users, num_treatment_users],     alternative='smaller')
print("A/B test p-value =", p_value)  


# lets now pretend all those in the treatment group converted...
from pyspark.sql.functions import *
df = df     .withColumn('converted_new',    when(df.group == "control", df.converted).otherwise(1))     .drop(df.converted)     .select(col('user_id'),             col('timestamp'),             col('group'),             col('landing_page'),             col('converted_new').alias('converted'))
df.show()


# recalculate the conversion rate for the treatment group
num_treatment_users = df.filter( df.group == "treatment").count()
num_treatment_converted =     df.filter( ( df.group == "treatment") & ( df.converted == 1) ).count()
print("treatment group conversion rate = ",     num_treatment_converted / num_treatment_users )


# recalculate the p-value
import statsmodels.api as sm
_, p_value = sm.stats.proportions_ztest(     [num_control_converted, num_treatment_converted],     [num_control_users, num_treatment_users],     alternative='smaller')
print("A/B test p-value =", p_value)  

