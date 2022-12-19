from datasets import load_dataset
dataset = load_dataset('oscar', "unshuffled_deduplicated_en", split='train', streaming=True)

shuffled_dataset = dataset.shuffle(seed=42, buffer_size=10000)

import nltk
from nltk import word_tokenize
import numpy as np
from tqdm import tqdm
import time

def is_punctuation(word:str):
    return not (word.isalpha() or word.isnumeric())

# MapReduce
import time
import os
from multiprocessing import Process,Manager,Pool

def Map_v1(sentences,k,lis):  #Map函数进行分词并存储到列表
    print('process %d start'%os.getpid())
    dic = {}
    
    for snt_idx,snt in enumerate(sentences):
        words = word_tokenize(snt['text'])
        # print('process %d working on sentence %d'%(os.getpid(),snt['id']))
        
        for wd in words:
            if is_punctuation(wd):
                continue
            wd = wd.lower()
            
            if len(dic.keys())<k-1 or wd in dic.keys():
                dic[wd] = dic.get(wd,0)+1
            else:
                del_keys = []
                
                for key in dic.keys():
                    if dic[key] == 1:
                        del_keys.append(key)
                    else:
                        dic[key] = dic[key] - 1
                        
                for key in del_keys:
                    del dic[key]
                
        if snt_idx % 2000 == 0:
            print('process %d has done %d sentences'%(os.getpid(),snt_idx))
    lis.append(dic)
    #print(len(lis))

def Reduce_v1(lis):  #Reduce函数将结果汇总到字典中
    # print('time2 = %f'%(time.time()-start_time))  #测试Map函数总耗时（分词总耗时）
    dic = {}
    
    for d in lis:
        for k,v, in tqdm(d.items()):
            dic[k] = dic.get(k,0)+v
    
    dic_order=sorted(dic.items(),key=lambda x:x[1],reverse=True)  #字典降序排序
    with open('log_mr_v1.txt','a',encoding='utf-8') as file:
        for k,v in dic_order:
            file.write(k+':'+str(v)+'\n')  #将结果写入文件

def Map_Reduce_v1(dataset,showed_k=100,k=1000,item_per_worker=10000,max_iter=100000):
    start_time = time.time()
    
    log_file = './log_mr_v1.txt'
    with open(log_file,'w') as f:
        f.truncate()

    plist = []

    m = Manager()
    managed_list = m.list([])
    
    print('time1 (Initialize) = %f'%(time.time()-start_time))  #测试提取文档用时
    
    for i in range(max_iter//item_per_worker):   #创建进程
        sample = dataset.take(item_per_worker)
        p = Process(target=Map_v1,args=(sample,k,managed_list))
        plist.append(p)

        dataset = dataset.skip(item_per_worker)
        print('time (Initialize process %d) = %f'%(i,time.time()-start_time))

    for p in plist:
        p.start()  #启动进程
    for p in plist:
        p.join()  #阻滞主进程
    print('time2 (Map) = %f'%(time.time()-start_time))  #测试Map函数总耗时
    
    Reduce_v1(managed_list) #当Map进程全部完成之后Reduce进行结果归约
    
    print('time3 (Reduce) = %f'%(time.time() - start_time))   #测试总用时
    
if __name__ == '__main__': 
    # Map_Reduce_v1(misra-greis multiprocess)
    # Map: 4522s
    # Reduce: 3979s
    Map_Reduce_v1(shuffled_dataset)