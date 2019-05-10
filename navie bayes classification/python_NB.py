# -*- coding: utf-8 -*-
"""
Created on Fri May 10 12:16:29 2019

@author: 93568
"""

from collections import defaultdict
import math
import os
import re

doc1_filenames={}
doc2_filenames={}

# postings[term] 表示在类别内term词的词频
postings1 = defaultdict(dict)
postings2 = defaultdict(dict)

#每个类别的总词数
num_c1=0
num_c2=0


total_aRate=0

def main():
    Newspath1=(r".\20news-18828_test\alt.atheism")
    Newspath2=(r".\20news-18828_test\comp.graphics")
    
    getClass_dic()
    initialize_terms_and_postings()
   
    #设定先验概率
    
    Pv1 = len(doc1_filenames)/(len(doc1_filenames)+len(doc2_filenames))
    Pv2 = len(doc2_filenames)/(len(doc1_filenames)+len(doc2_filenames))
    
    print("先验概率：",Pv1,Pv2)
    print("类别词表长度",len(postings1),len(postings2))
    print("类别score计算分母：",num_c1+len(postings1),num_c2+len(postings2))
    test(Newspath1,0,Pv1,Pv2)
    test(Newspath2,1,Pv1,Pv2)

    print("平均准确率为：",total_aRate/2)
    
def initialize_terms_and_postings():
    global postings1,postings2,num_c1,num_c2
    
    for id in doc1_filenames:
        f = open(doc1_filenames[id],'r',encoding='utf-8',errors='ignore')
        lines = f.readlines()
        f.close()
        
        terms = []
        
        for line in lines:
            line = re.sub(r"[^a-zA-Z]"," ",line)
            line = re.sub(r"\\s{2,}"," ",line)
            line = line.strip().lower()
            words = line.split(" ")
            for word in words:
                if len(word)>0:
                    terms.append(word)
        
        num_c1+=len(terms)#类1总词数
        unique_terms = set(terms)
        for term in unique_terms:
            if term not in postings1:
                postings1[term] = (terms.count(term))
            else:
                postings1[term] =(postings1[term]+(terms.count(term)))
    
    
    for id in doc2_filenames:
        f = open(doc2_filenames[id],'r',encoding='utf-8',errors='ignore')
        lines = f.readlines()
        f.close()
        
        terms = []
        
        for line in lines:
            line = re.sub(r"[^a-zA-Z]"," ",line)
            line = re.sub(r"\\s{2,}"," ",line)
            line = line.strip().lower()
            words = line.split(" ")
            for word in words:
                if len(word)>0:
                    terms.append(word)
            
        num_c2+=len(terms)#类2总词数
        unique_terms = set(terms)
        for term in unique_terms:
            if term not in postings2:
                postings2[term] = (terms.count(term))
            else:
                postings2[term] =(postings2[term]+(terms.count(term)))
                

def getClass_dic():
    #小规模测试文档路径
    global doc1_filenames,doc2_filenames
    Newspath1=(r".\20news-18828_train\alt.atheism")
    Newspath2=(r".\20news-18828_train\comp.graphics")

    files=[f for f in  os.listdir(Newspath1)]
    i=0
    for fi in files:       
        doc1_filenames.update({i:os.path.join(Newspath1,fi)})
        i+=1

    files=[f for f in  os.listdir(Newspath2)]
    i=0
    for fi in files:       
        doc2_filenames.update({i:os.path.join(Newspath2,fi)})
        i+=1

   
    
def test(Newspath1,ss,Pv1,Pv2):
    global total_aRate
    nc1=len(postings1)
    nc2=len(postings2)

    
    doc1_test={}
    files=[f for f in  os.listdir(Newspath1)]
    i=0
    for fi in files:       
        doc1_test.update({i:os.path.join(Newspath1,fi)})
        i+=1
    
    count1=0
    

    
    
    for id in doc1_test:
        f = open(doc1_test[id],'r',encoding='utf-8',errors='ignore')
        lines = f.readlines()
        f.close()
        
        terms = []
        
        for line in lines:
            line = re.sub(r"[^a-zA-Z]"," ",line)
            line = re.sub(r"\\s{2,}"," ",line)
            line = line.strip().lower()
            terms.extend(line.split(" "))
            
        p=[0,0]
        for term in terms:
            if term in postings1:
                p[0]+=math.log((postings1[term]+1.0)/(num_c1+nc1))
            else:
                p[0]+=math.log(1.0/(num_c1+nc1))
                
            if term in postings2:
                p[1]+=math.log((postings2[term]+1.0)/(num_c2+nc2))
            else:
                p[1]+=math.log(1.0/(num_c2+nc2))
                
        p[0]+=math.log(Pv1)
        p[1]+=math.log(Pv2)
        
        if p[ss]==max(p):
            count1+=1
    print(ss+1,"类名：",Newspath1[70:])
    print("判对文档数：",count1,"总的文档数：",len(doc1_test))
    total_aRate = (total_aRate+count1/len(doc1_test))
    print("准确率为：",count1/len(doc1_test))
    
    
if __name__ == "__main__":
    main()

