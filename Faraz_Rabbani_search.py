import sys
import json
inFile = sys.argv[1]
ff=str(inFile)

word=sys.argv[2]

with open (ff) as f:
    jsondata=json.load(f)
l=[]
for row in jsondata['data']:
    for i in row['paragraphs']:
        for j in i['qas']:
            cv=0

            for kk in word.split():
                if kk.lower() in j['question'].lower().split():
                    cv+=1
            if cv==len(word.split()):


                dic={}
                dic['id']=j['id']
                dic['question']=j['question']
                dic['answer']=j['answers'][0]['text']
                l.append(dic)

sys.stdout.write(str(len(l))+'\n')
                
with open('2a.json', 'w') as fp:
    json.dump(l, fp)