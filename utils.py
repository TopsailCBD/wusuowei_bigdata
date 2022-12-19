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
            
    with open(file.split[-4]+'_new.txt','a',encoding='utf-8') as file:
        for k,v in frequency:
            file.write(k+':'+str(v)+'\n')  #将结果写入文件

def replace_keys(good_value_dict:dict,good_key_dict:dict):
    better_dict = {}
    
    good_value_dict = sorted(good_value_dict.items(),key=lambda x:x[1],reverse=True)
    good_key_dict = sorted(good_key_dict.items(),key=lambda x:x[1],reverse=True)
    
    for (_,good_value),(good_key,_) in zip(good_value_dict,good_key_dict):
        better_dict[good_key] = good_value
        
    return better_dict

def calculate_key_cover_rate(gt_dict:dict,pred_dict:dict):
    matched_keys = 0
    
    for gt_key in gt_dict.keys():
        if gt_key in pred_dict.keys():
            matched_keys += 1
    
    return float(matched_keys)/len(gt_dict.keys())    

def read_dict_from_result_file(file, topk=100):
    d = {}
    with open(file, 'r', encoding='utf-8') as f:
        for line_idx,line in enumerate(f):
            word, freq = line.split(':')
            freq = int(freq)
            d[word] = freq
            
            if line_idx+1 >= topk:
                break
    return d

def read_dict_from_log_file(file, topk=100):
    d = {}
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        last_line = lines[-1]
        
        valid_pairs = 0
        
        for word_freq in last_line.split():
            word_freq_pair = word_freq.split(':')
            
            if len(word_freq_pair) == 2:
                word, freq = word_freq_pair
                if len(freq) == 0: # 'xxx:'会被分割成['xxx','']
                    continue
                freq = int(freq)
                d[word] = freq
                
                valid_pairs += 1
                if valid_pairs >= topk:
                    break
                
    return d
     
# correct_capitalization('./log_mr_v1.txt')