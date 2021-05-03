#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession 
from pyspark.sql.types import *
#spark = SparkSession.builder.getOrCreate() # Our session manager
spark = SparkSession.builder.config('spark.driver.memory','5g').config("spark.blaclist.enabled","false").getOrCreate()

sc = spark.sparkContext # Our Spark Context (gives us access to functions)


# In[2]:


from splicemachine.notebook import get_spark_ui
get_spark_ui()


# In[3]:

#censored our access and secret keys
spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "——————————————————")
spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "——————————————————————————"


# In[4]:


import os
import pandas as pd
get_ipython().system('pip install s3fs')
get_ipython().system('pip install --upgrade fsspec')


# In[5]:


os.environ["AWS_ACCESS_KEY_ID"] = "——————————————————"
os.environ["AWS_SECRET_ACCESS_KEY"] = "——————————————————————————"


# In[7]:


df = pd.read_csv('s3a://cse427-finalproject/VAERS/2021VAERSVAX.csv', encoding = "ISO-8859-1")
df1 = pd.read_csv('s3a://cse427-finalproject/VAERS/2021VAERSSYMPTOMS.csv', encoding = "ISO-8859-1")
df2 = pd.read_csv('s3a://cse427-finalproject/VAERS/2021VAERSData.csv', encoding = "ISO-8859-1")


# In[8]:


schema = StructType([
    StructField('VAERS_ID', IntegerType()),
    StructField('VAX_TYPE', StringType()),
    StructField('VAX_MANU', StringType()),
    StructField('VAX_LOT', StringType()),
    StructField('VAX_DOSE_SERIES', StringType()),
    StructField('VAX_ROUTE', StringType()),
    StructField('VAX_SITE', StringType()),
    StructField('VAX_NAME', StringType())
])
# vaccine table
spark_df = spark.createDataFrame(df, schema=schema)
spark_df.show(10)


# In[9]:


schema_1 = StructType([
    StructField('VAERS_ID', IntegerType()),
    StructField('SYMPTOM1', StringType()),
    StructField('SYMPTOMVERSION1', DoubleType()),
    StructField('SYMPTOM2', StringType()),
    StructField('SYMPTOMVERSION2', DoubleType()),
    StructField('SYMPTOM3', StringType()),
    StructField('SYMPTOMVERSION3', DoubleType()),
    StructField('SYMPTOM4', StringType()),
    StructField('SYMPTOMVERSION4', DoubleType()),
    StructField('SYMPTOM5', StringType()),
    StructField('SYMPTOMVERSION5', DoubleType())
])
spark_df1 = spark.createDataFrame(df1, schema=schema_1)
spark_df1.show(10)


# In[10]:


schema2 = StructType([
    StructField('VAERS_ID', IntegerType()),
    StructField('RECVDATE', StringType()),
    StructField('STATE', StringType()),
    StructField('AGE_YRS', StringType()),
    StructField('CAGE_YR', StringType()),
    StructField('CAGE_MO', StringType()),
    StructField('SEX', StringType()),
    StructField('RPT_DATE', StringType()),
    StructField('DIED'  , StringType()),
    StructField('DATEDIED'  , StringType()),
    StructField('L_THREAT'  , StringType()),
    StructField('ER_VISIT'  , StringType()),
    StructField('HOSPITAL'  , StringType()),
    StructField('HOSPDAYS'  , StringType()),
    StructField('X_STAY'   , StringType()),
    StructField('DISABLE' , StringType()),
    StructField('RECOVD'  , StringType()),
    StructField('VAX_DATE' , StringType()),
    StructField('ONSET_DATE' , StringType()),
    StructField('NUMDAYS'  , StringType()),
    StructField('LAB_DATA'   , StringType()),
    StructField('V_ADMINBY', StringType()),
    StructField('V_FUNDBY'   , StringType()),
    StructField('OTHER_MEDS'  , StringType()),
    StructField('CUR_ILL'   , StringType()),
    StructField('HISTORY'    , StringType()),
    StructField('PRIOR_VAX' , StringType()),
    StructField('SPLTTYPE'   , StringType()),
    StructField('FORM_VERS' , IntegerType()),
    StructField('TODAYS_DATE' , StringType()),
    StructField('BIRTH_DEFECT' , StringType()),
    StructField('OFC_VISIT' , StringType()),
    StructField('ER_ED_VISIT' , StringType()),
    StructField('ALLERGIES'   , StringType()),
    ])

spark_df2 = spark.createDataFrame(df2, schema=schema2)
spark_df2.show(10)
df2.dtypes


# In[11]:


import pyspark.sql.functions as f
spark_df.registerTempTable('spark_df')
spark_df1.registerTempTable('spark_df1')
spark_df2.registerTempTable('spark_df2')


# In[12]:


#current issue: there are duplicate rows of VAERS_ID in both df1 and df2
numVaccineData = spark.sql('''
    select spark_df.VAERS_ID, STATE, CAGE_YR, SEX ,DIED, RECOVD, L_THREAT, VAX_TYPE, VAX_MANU, SYMPTOM1, SYMPTOM2, SYMPTOM3, SYMPTOM4 from spark_df
    inner join spark_df1 on spark_df1.VAERS_ID = spark_df.VAERS_ID
    inner join spark_df2 on spark_df2.VAERS_ID = spark_df.VAERS_ID
    where VAX_TYPE = "COVID19" and CAGE_YR != "NaN"
''')
numVaccineData.show()
numVaccineData.registerTempTable('numVaccineData')


# In[13]:


#Moderna Data:
modernaVaccineData = spark.sql('''
    select spark_df.VAERS_ID, STATE, CAGE_YR, SEX ,VAX_TYPE, VAX_MANU,
    SYMPTOM1, SYMPTOM2, SYMPTOM3, SYMPTOM4 from spark_df
    inner join spark_df1 on spark_df1.VAERS_ID = spark_df.VAERS_ID
    inner join spark_df2 on spark_df2.VAERS_ID = spark_df.VAERS_ID
    where VAX_MANU = "MODERNA" and CAGE_YR != "NaN"
''')
modernaVaccineData.show()

#symptoms:
modernaVaccineData.registerTempTable('modernaVaccineData')
modernaSymptoms = spark.sql('''
    SELECT SYMPTOM1 FROM modernaVaccineData
    UNION ALL
    SELECT SYMPTOM2 FROM modernaVaccineData
    UNION ALL
    SELECT SYMPTOM3 FROM modernaVaccineData
    UNION ALL
    SELECT SYMPTOM4 FROM modernaVaccineData
''')
modernaSymptoms.show()
modernaSymptoms.count()


#count and frequency
modernaSymptoms.registerTempTable('modernaSymptoms')
numTotal = modernaSymptoms.count()
modernaSymptomsCount = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM modernaSymptoms
    where SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

modernaSymptomsCount.show()
modernaSymptomsCount.count()


# In[14]:


import s3fs
test1 = modernaSymptomsCount.toPandas()
test1.head()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/modernaSymptomsCount.csv','w') as f:
    test1.to_csv(f)


# In[69]:


#moderna for each age group
print("Child Data - Moderna")

modernaSymptoms1 = spark.sql('''
    SELECT SYMPTOM1 FROM modernaVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM modernaVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM3 FROM modernaVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM4 FROM modernaVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
modernaSymptoms1.show()

#count and frequency
modernaSymptoms1.registerTempTable('modernaSymptoms1')
numTotal = modernaSymptoms1.count()
modernaSymptomsCount1 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM modernaSymptoms1
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

modernaSymptomsCount1.show()
modernaSymptomsCount1.count()

#moderna for each age group
print("Youth Data - Moderna")
modernaSymptoms2 = spark.sql('''
    SELECT SYMPTOM1 FROM modernaVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM modernaVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM modernaVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM modernaVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')


#modernaSymptoms2.show()

#count and frequency
modernaSymptoms2.registerTempTable('modernaSymptoms2')
numTotal = modernaSymptoms2.count()
print(numTotal)
modernaSymptomsCount2 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM modernaSymptoms2
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

modernaSymptomsCount2.show()
modernaSymptomsCount2.count()


#moderna for each age group
print("Adult Data - Moderna")
modernaSymptoms3 = spark.sql('''
    SELECT SYMPTOM1 FROM modernaVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM modernaVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM modernaVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM modernaVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')

modernaSymptoms3.count()
#modernaSymptomsTemp3.show()

#count and frequency
modernaSymptoms3.registerTempTable('modernaSymptoms3')
numTotal = modernaSymptoms3.count()
modernaSymptomsCount3 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM modernaSymptoms3
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

modernaSymptomsCount3.show()
modernaSymptomsCount3.count()


#moderna for each age group
print("Senior Data - Moderna")
modernaSymptoms4 = spark.sql('''
    SELECT SYMPTOM1 FROM modernaVaccineData
    where CAGE_YR >= 65 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM modernaVaccineData
    where CAGE_YR >= 65 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM modernaVaccineData
    where CAGE_YR >= 65  and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM modernaVaccineData
    where CAGE_YR >= 65 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')

modernaSymptoms4.count()
#modernaSymptoms4.show()

#count and frequency
modernaSymptoms4.registerTempTable('modernaSymptoms4')
numTotal = modernaSymptoms4.count()
modernaSymptomsCount4 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM modernaSymptoms4
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

modernaSymptomsCount4.show()
modernaSymptomsCount4.count()


# In[70]:


test1 = modernaSymptomsCount1.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/modernaSymptomsCountChild.csv','w') as f:
    test1.to_csv(f)
    
test1 = modernaSymptomsCount2.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/modernaSymptomsCountYouth.csv','w') as f:
    test1.to_csv(f)   
test1 = modernaSymptomsCount3.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/modernaSymptomsCountAdult.csv','w') as f:
    test1.to_csv(f)
    
test1 = modernaSymptomsCount4.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/modernaSymptomsCountSenior.csv','w') as f:
    test1.to_csv(f)   


# In[71]:


#JANSSEN Data:
JANSSENVaccineData = spark.sql('''
    select spark_df.VAERS_ID, STATE, CAGE_YR, SEX ,VAX_TYPE, VAX_MANU, SYMPTOM1, SYMPTOM2, SYMPTOM3, SYMPTOM4 from spark_df
    inner join spark_df1 on spark_df1.VAERS_ID = spark_df.VAERS_ID
    inner join spark_df2 on spark_df2.VAERS_ID = spark_df.VAERS_ID
    where VAX_MANU = "JANSSEN" and CAGE_YR != "NaN" 
''')
JANSSENVaccineData.show()

#symptoms:
JANSSENVaccineData.registerTempTable('JANSSENVaccineData')
JANSSENSymptoms = spark.sql('''
    SELECT SYMPTOM1 FROM JANSSENVaccineData
    UNION ALL
    SELECT SYMPTOM2 FROM JANSSENVaccineData
    UNION ALL
    SELECT SYMPTOM3 FROM JANSSENVaccineData
    UNION ALL
    SELECT SYMPTOM4 FROM JANSSENVaccineData
''')     
JANSSENSymptoms.show()
JANSSENSymptoms.count()

#Count and frequency
JANSSENSymptoms.registerTempTable('JANSSENSymptoms')
numTotal = JANSSENSymptoms.count()
JANSSENSymptomsCount = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM JANSSENSymptoms
    where SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))
JANSSENSymptomsCount.show()
JANSSENSymptomsCount.count()


# In[72]:


test1 = JANSSENSymptomsCount.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/JANSSENSymptomsCount.csv','w') as f:
    test1.to_csv(f)


# In[73]:


#JANSSEN for each age group
print("Child Data - JANSSEN")
JANSSENSymptoms1 = spark.sql('''
    SELECT SYMPTOM1 FROM JANSSENVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM JANSSENVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM JANSSENVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM JANSSENVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#JANSSENSymptoms1.show()
JANSSENSymptoms1.count()
#count and frequency
JANSSENSymptoms1.registerTempTable('JANSSENSymptoms1')
numTotal = JANSSENSymptoms1.count()
JANSSENSymptomsCount1 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM JANSSENSymptoms1
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

JANSSENSymptomsCount1.show()
JANSSENSymptomsCount1.count()

#JANSSEN for each age group
print("Youth Data - JANSSEN")
JANSSENSymptoms2 = spark.sql('''
    SELECT SYMPTOM1 FROM JANSSENVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM JANSSENVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM JANSSENVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM JANSSENVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#JANSSENSymptoms2.show()
JANSSENSymptoms2.count()
#count and frequency
JANSSENSymptoms2.registerTempTable('JANSSENSymptoms2')
numTotal = JANSSENSymptoms2.count()
JANSSENSymptomsCount2 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM JANSSENSymptoms2
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

JANSSENSymptomsCount2.show()
JANSSENSymptomsCount2.count()


#JANSSEN for each age group
print("Adult Data - JANSSEN")
JANSSENSymptoms3 = spark.sql('''
    SELECT SYMPTOM1 FROM JANSSENVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM JANSSENVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM JANSSENVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM JANSSENVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#JANSSENSymptoms3.show()
JANSSENSymptoms3.count()
#count and frequency
JANSSENSymptoms3.registerTempTable('JANSSENSymptoms3')
numTotal = JANSSENSymptoms3.count()
JANSSENSymptomsCount3 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM JANSSENSymptoms3
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

JANSSENSymptomsCount3.show()
JANSSENSymptomsCount3.count()


#JANSSEN for each age group
print("Senior Data - JANSSEN")
JANSSENSymptoms4 = spark.sql('''
    SELECT SYMPTOM1 FROM JANSSENVaccineData
    where CAGE_YR >= 65 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM JANSSENVaccineData
    where CAGE_YR >= 65 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM JANSSENVaccineData
    where CAGE_YR >= 65 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM JANSSENVaccineData
    where CAGE_YR >= 65 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#JANSSENSymptoms4.show()
JANSSENSymptoms4.count()
#count and frequency
JANSSENSymptoms4.registerTempTable('JANSSENSymptoms4')
numTotal = JANSSENSymptoms4.count()
JANSSENSymptomsCount4 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM JANSSENSymptoms4
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

JANSSENSymptomsCount4.show()
JANSSENSymptomsCount4.count()


# In[74]:


test1 = JANSSENSymptomsCount1.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/JANSSENSymptomsCountChild.csv','w') as f:
    test1.to_csv(f)
    
test1 = JANSSENSymptomsCount2.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/JANSSENSymptomsCountYouth.csv','w') as f:
    test1.to_csv(f)   
test1 = JANSSENSymptomsCount3.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/JANSSENSymptomsCountAdult.csv','w') as f:
    test1.to_csv(f)
    
test1 = JANSSENSymptomsCount4.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/JANSSENSymptomsCountSenior.csv','w') as f:
    test1.to_csv(f)   


# In[75]:


#pfizer
pfizerVaccineData = spark.sql('''
    select spark_df.VAERS_ID, STATE, CAGE_YR, SEX ,VAX_TYPE, VAX_MANU, SYMPTOM1, SYMPTOM2, SYMPTOM3, SYMPTOM4 from spark_df
    inner join spark_df1 on spark_df1.VAERS_ID = spark_df.VAERS_ID
    inner join spark_df2 on spark_df2.VAERS_ID = spark_df.VAERS_ID
    where VAX_MANU LIKE "PFIZER%" and CAGE_YR != "NaN" 
''')
pfizerVaccineData.show()

#symptoms:
pfizerVaccineData.registerTempTable('pfizerVaccineData')
pfizerSymptoms = spark.sql('''
    SELECT SYMPTOM1 FROM pfizerVaccineData
    UNION ALL
    SELECT SYMPTOM2 FROM pfizerVaccineData
    UNION ALL
    SELECT SYMPTOM3 FROM pfizerVaccineData
    UNION ALL
    SELECT SYMPTOM4 FROM pfizerVaccineData
''')     
pfizerSymptoms.show()
pfizerSymptoms.count()

#Count and frequency
pfizerSymptoms.registerTempTable('pfizerSymptoms')
numTotal = pfizerSymptoms.count()
pfizerSymptomsCount = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM pfizerSymptoms
    where SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))
pfizerSymptomsCount.show()
pfizerSymptomsCount.count()


# In[76]:


test1 = pfizerSymptomsCount.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/pfizerSymptomsCount.csv','w') as f:
    test1.to_csv(f)


# In[77]:


#pfizer for each age group
print("Child Data - pfizer")
pfizerSymptoms1 = spark.sql('''
    SELECT SYMPTOM1 FROM pfizerVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM pfizerVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM pfizerVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM pfizerVaccineData
    where CAGE_YR >= 1 and CAGE_YR <= 14 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#pfizerSymptoms1.show()
pfizerSymptoms1.count()
#count and frequency
pfizerSymptoms1.registerTempTable('pfizerSymptoms1')
numTotal = pfizerSymptoms1.count()
pfizerSymptomsCount1 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM pfizerSymptoms1
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

pfizerSymptomsCount1.show()
pfizerSymptomsCount1.count()

#pfizer for each age group
print("Youth Data - pfizer")
pfizerSymptoms2 = spark.sql('''
    SELECT SYMPTOM1 FROM pfizerVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM pfizerVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM pfizerVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM pfizerVaccineData
    where CAGE_YR >= 15 and CAGE_YR <= 24 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#pfizerSymptoms2.show()
pfizerSymptoms2.count()
#count and frequency
pfizerSymptoms2.registerTempTable('pfizerSymptoms2')
numTotal = pfizerSymptoms2.count()
pfizerSymptomsCount2 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM pfizerSymptoms2
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

pfizerSymptomsCount2.show()
pfizerSymptomsCount2.count()


#pfizer for each age group
print("Adult Data - pfizer")
pfizerSymptoms3 = spark.sql('''
    SELECT SYMPTOM1 FROM pfizerVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM pfizerVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM pfizerVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM pfizerVaccineData
    where CAGE_YR >= 25 and CAGE_YR <= 64 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#pfizerSymptoms3.show()
pfizerSymptoms3.count()
#count and frequency
pfizerSymptoms3.registerTempTable('pfizerSymptoms3')
numTotal = pfizerSymptoms3.count()
pfizerSymptomsCount3 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM pfizerSymptoms3
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

pfizerSymptomsCount3.show()
pfizerSymptomsCount3.count()


#pfizer for each age group
print("Senior Data - pfizer")
pfizerSymptoms4 = spark.sql('''
    SELECT SYMPTOM1 FROM pfizerVaccineData
    where CAGE_YR >= 65 and SYMPTOM1 != "Product administered to patient of inappropriate age" and SYMPTOM1 != "NaN" and SYMPTOM1 != "No adverse event" 
    UNION ALL
    SELECT SYMPTOM2 FROM pfizerVaccineData
    where CAGE_YR >= 65 and SYMPTOM2 != "Product administered to patient of inappropriate age" and SYMPTOM2 != "NaN" and SYMPTOM2 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM3 FROM pfizerVaccineData
    where CAGE_YR >= 65 and SYMPTOM3 != "Product administered to patient of inappropriate age" and SYMPTOM3 != "NaN" and SYMPTOM3 != "No adverse event"
    UNION ALL
    SELECT SYMPTOM4 FROM pfizerVaccineData
    where CAGE_YR >= 65 and SYMPTOM4 != "Product administered to patient of inappropriate age" and SYMPTOM4 != "NaN" and SYMPTOM4 != "No adverse event" 
''')
#pfizerSymptoms4.show()
pfizerSymptoms4.count()
#count and frequency
pfizerSymptoms4.registerTempTable('pfizerSymptoms4')
numTotal = pfizerSymptoms4.count()
pfizerSymptomsCount4 = spark.sql('''
    SELECT SYMPTOM1, count(*) as symptom_count, count(*)/{} as freq FROM pfizerSymptoms4
    group by SYMPTOM1
    order by symptom_count DESC
'''.format(numTotal))

pfizerSymptomsCount4.show()
pfizerSymptomsCount4.count()


# In[78]:


test1 = pfizerSymptomsCount1.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/pfizerSymptomsCountChild.csv','w') as f:
    test1.to_csv(f)
    
test1 = pfizerSymptomsCount2.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/pfizerSymptomsCountYouth.csv','w') as f:
    test1.to_csv(f)   
test1 = pfizerSymptomsCount3.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/pfizerSymptomsCountAdult.csv','w') as f:
    test1.to_csv(f)
    
test1 = pfizerSymptomsCount4.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/pfizerSymptomsCountSenior.csv','w') as f:
    test1.to_csv(f)   


# In[61]:


#Query2 - Analyze
#How many people died after taking the COVID vaccines?
q_2_d = spark.sql('''
select count(DISTINCT VAERS_ID) as num_ppl_dead from numVaccineData
WHERE DIED == "Y"
''')
q_2_d.show()

#For each COVID vaccine
q_2_v = spark.sql('''
select VAX_MANU, count(DISTINCT VAERS_ID) from numVaccineData
where DIED = "Y"
group by VAX_MANU
''')
q_2_v.show() 


# In[62]:


test1 = q_2_v.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/peopledied_per_vaccine.csv','w') as f:
    test1.to_csv(f)


# In[40]:


#Sub_query_1
#How many types of VACCINE? There are only 4
sub_q_1 = spark_df
sub_q_1.registerTempTable('sub_q_1')
sub_q_1 = spark.sql('''
select VAX_MANU, count(*)
from sub_q_1
where VAX_TYPE = "COVID19"
group by VAX_MANU
''')
sub_q_1.show()
#How many people take for each of VACCINE?
sub_q_1 = spark_df
sub_q_1.registerTempTable('sub_q_1')
sub_q_1 = spark.sql('''
select VAX_MANU, count(*)
from sub_q_1
where VAX_TYPE = "COVID19" 
group by VAX_MANU
''')
sub_q_1.show() 


# In[42]:


test1 = sub_q_1.toPandas()
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('cse427-finalproject/Results/ppl_each_vaccine.csv','w') as f:
    test1.to_csv(f)


# In[18]:


numVaccineData.registerTempTable('numVaccineData')
#Sub Query 4
#How many people are in each age group?
#0-14 Children, 15-24 Youth, 25-64 Adults, 65+ Seniors
#CHILDREN
Children = spark.sql('''
select count(*) as Children
from numVaccineData
where CAGE_YR >= 1
and CAGE_YR <= 14
''')
Children.show()
#YOUTH
Youth = spark.sql('''
select count(*) as Youth
from numVaccineData
where CAGE_YR >= 15
and CAGE_YR <= 24
''')
Youth.show()
#ADULTS
Adults = spark.sql('''
select count(*) as Adults
from numVaccineData
where  CAGE_YR >= 25
and CAGE_YR <= 64
''')
Adults.show()
#SENIORS
Seniors = spark.sql('''
select count(*) as Seniors
from numVaccineData
where CAGE_YR >= 65
''')
Seniors.show()


# In[ ]:


spark.stop()

