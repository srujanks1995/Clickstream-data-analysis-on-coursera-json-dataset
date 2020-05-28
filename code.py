import pyspark 
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
df = spark.read.json("psy-001_clickstream_export.json") 
df.printSchema()  


#Most active users

a=df.select('username')
a=a.toPandas()
grouped=a.username.groupby(a.username).count()
a=grouped.sort_values( ascending=False).head(20)
a.to_csv('Most active users.csv')  

#Most viewed videos

f=df.select('key','page_url')
f=f.toPandas()
filter = f["key"]=="user.video.lecture.action"
i=f[filter]
i=i.iloc[:,1]
grouped=i.groupby(i).count()
i=grouped.sort_values( ascending=False).head(20)
i.to_csv('Most viewed videos.csv') 


#Most Used Internet Browser or Device

u=df.select('user_agent')
u=u.toPandas()
grouped=u.user_agent.groupby(u.user_agent).count()
u=grouped.sort_values( ascending=False).head(20)
u.to_csv('Most Used Internet Browser or Device.csv')  


#Most used language

l=df.select('language')
l=l.toPandas()
grouped=l.language.groupby(l.language).count()
l=grouped.sort_values( ascending=False).head(20)
l.to_csv('Most used language.csv') 

#Peak usage time

t=df.select('timestamp')
t=t.toPandas()
grouped=t.timestamp.groupby(t.timestamp).count()
t=grouped.sort_values( ascending=False).head(20)
t.to_csv('Peak usage time.csv')  






