from pyspark import SparkConf, SparkContext
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import findspark
findspark.init()
% matplotlib inline


conf = SparkConf().setMaster("local").setAppName("Yelp Dataset Analysis")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///Users/yashparikh/Project/dataset/business.json")
data_lines = input.map(lambda x: json.loads(x))
stars = data_lines.map(lambda x: (x["stars"], 1))
count_stars = stars.reduceByKey(lambda x,y: x+y).collectAsMap() #this is a dictionary
#save dict to a file

# json = json.dumps(count_stars)
# f = open("dict.json","w")
# f.write(json)
# f.close()


# f = '/Users/shakti/Downloads/spark-2.3.0-bin-hadoop2.7/dict.json'
# file = open(f)
# myfile=file.read()
# jsonData=json.loads(myfile)
df = pd.DataFrame(count_stars, index=range(0,1))
df

sns.countplot(x="2.0",data=df)

# print (type(count_stars))
# df = pd.DataFrame.from_dict(count_stars)
# print type(df)
# # print "stars and their values" , count_stars[1]
# for s,v in count_stars.items():
# print "star",  s
# print "values", v
# # print "value" , v