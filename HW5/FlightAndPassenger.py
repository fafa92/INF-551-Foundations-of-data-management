from pyspark import SparkConf, SparkContext
sc =SparkContext.getOrCreate()
from operator import add


lax=sc.textFile("lax_flights.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[3],line[4],line[5])).collect()


pas=sc.textFile("lax_passengers.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[3],line[4],line[5])).collect()

def cor(x):
    d=x[0].split('/')
    dd=""
    dd+=d[0]+'/'+d[2]
    dd=dd.split()
    dd=str(dd[0])

    dd+=' '+ x[1]+' '+x[2]
    dd=str(dd)
    return (dd,int(x[3]))
    
ds1=sc.parallelize(lax).map(cor).filter(lambda x: x is not None).reduceByKey(add).collect()
ds2=sc.parallelize(pas).map(cor).filter(lambda x: x is not None).reduceByKey(add).collect()
ds1=sc.parallelize(ds1)
ds2=sc.parallelize(ds2)
ds1.join(ds2).collect()