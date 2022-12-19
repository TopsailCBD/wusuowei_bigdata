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

def Map_v0(sentences,lis):  #Map函数进行分词并存储到列表
    print('process %d start'%os.getpid())
    for snt_idx,snt in enumerate(sentences):
        words = word_tokenize(snt['text'])
        
        # print('process %d working on sentence %d'%(os.getpid(),snt['id']))
        
        for wd in words:
            if not is_punctuation(wd):
                lis.append((wd.lower(),1))
                
        if snt_idx % 2000 == 0:
            print('process %d has done %d sentences'%(os.getpid(),snt_idx))
    #print(len(lis))

def Reduce_v0(lis):  #Reduce函数将结果汇总到字典中
    # print('time2 = %f'%(time.time()-start_time))  #测试Map函数总耗时（分词总耗时）
    dic = {}
    for k,v in tqdm(lis):
        dic[k] = dic.get(k,0)+1
        #print(dic)
    dic_order=sorted(dic.items(),key=lambda x:x[1],reverse=True)  #字典降序排序
    with open('log_mr_v0.txt','a',encoding='utf-8') as file:
        for k,v in dic_order:
            file.write(k+':'+str(v)+'\n')  #将结果写入文件

def Map_Reduce_v0(dataset,item_per_worker=10000,max_iter=100000):
    start_time = time.time()
    
    log_file = './log_mr_v0.txt'
    with open(log_file,'w') as f:
        f.truncate()

    plist = []
    # 用Pool改写一下，别用list of Process了
    # pool = Pool(num_workers)
    m = Manager()
    managed_list = m.list([])
    
    print('time1 (Initialize) = %f'%(time.time()-start_time))  #测试提取文档用时
    
    for i in range(max_iter//item_per_worker):   #创建进程
        sample = dataset.take(item_per_worker)
        p = Process(target=Map_v0,args=(sample,managed_list))
        plist.append(p)
        # pool.apply_async(Map_v0,args=(sample,managed_list))
        dataset = dataset.skip(item_per_worker)
        print('time (Initialize process %d) = %f'%(i,time.time()-start_time))
    # pool.close()
    # pool.join()  
    for p in plist:
        p.start()  #启动进程
    for p in plist:
        p.join()  #阻滞主进程
    print('time2 (Map) = %f'%(time.time()-start_time))  #测试Map函数总耗时
    
    Reduce_v0(managed_list) #当Map进程全部完成之后Reduce进行结果归约
    
    print('time3 (Reduce) = %f'%(time.time() - start_time))   #测试总用时
    
if __name__ == '__main__': 
    # Map_Reduce_v0: 2h22m23s
    # Map: 4522s
    # Reduce: 3979s
    Map_Reduce_v0(shuffled_dataset)