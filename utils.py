import os
from tqdm import tqdm

def correct_capitalization(file:str):
    frequency = {}

    with open(file,'r',encoding='utf-8') as f:
        for line in tqdm(f):
            word,freq = line.split(':')
            word = word.lower()
            freq = int(freq)
            frequency[word] = frequency.get(word,0)+freq
            
    frequency = sorted(frequency.items(),key=lambda x:x[1],reverse=True)
            
    with open(file.split('.')[0]+'_new.txt','a',encoding='utf-8') as file:
        for k,v in frequency:
            file.write(k+':'+str(v)+'\n')  #将结果写入文件
            
correct_capitalization('./log_mr_v1.txt')