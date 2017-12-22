from pyspark import SparkConf, SparkContext
sc =SparkContext.getOrCreate()
from operator import add

def date(x):
    if x[1] in ["Terminal 1" ,"Terminal 2","Terminal 3", "Terminal 4", "Terminal 5","Terminal 6","Terminal 7","Terminal 8","Tom Bradley International Terminal"]:
        d=x[0].split('/')
        dd=""
        dd+=d[0]+'/'+d[2]
        dd=dd.split()
 
        
        return (str(dd[0]),int(x[2]))
        
    
    
data=sc.textFile("lax_passengers.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[2],line[5])).collect()
sc.parallelize(data).map(date).filter(lambda x: x is not None).reduceByKey(add).filter(lambda (x,y):y>=5000000).collect()
