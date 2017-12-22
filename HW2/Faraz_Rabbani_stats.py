import sys
import json
inFile = sys.argv[1]
ff=str(inFile)
fff=ff+"1a.json"
#file = open(fff,"w") 
 

 
Q={'how':0,'how many':0,'how much':0,'what':0,'when':0,'where':0,'which':0,'who':0,'whom':0}
g=[]
with open (ff) as f:
    jsondata=json.load(f)
for row in jsondata['data']:
    for i in row['paragraphs']:
        for j in i['qas']:
            if j['question'].lower().split()[0] in Q:
                
                Q[j['question'].lower().split()[0]]+=1
                  
                  
for row in jsondata['data']:
    for i in row['paragraphs']:
        for j in i['qas']:
            if ' '.join(j['question'].lower().split()[:2]) in Q:
                
                Q[' '.join(j['question'].lower().split()[:2])]+=1
                
with open('1a.json', 'w') as fp:
    json.dump(Q, fp)


                