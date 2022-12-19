import os
import time

import matplotlib.pyplot as plt
import numpy as np

from utils import *

plt.rcParams['font.sans-serif'] = 'SimHei'
plt.rcParams['axes.unicode_minus'] = False

def plot_residual(gt:dict,pred:dict,equalized=False,title=''):
    fig = plt.figure(figsize=(8,4))
    
    # 初始化所有画柱形图所需量
    gt_x = []
    gt_height = []
    
    err1_x = []
    err1_height = []
    err1_bottom = []
    
    err2_x = []
    err2_height = []
    err2_bottom = []
    
    # 统计所有画柱形图所需量
    for word_idx,(key,gt_value) in enumerate(gt.items()):
        pred_value = pred.get(key,0)
        
        if equalized:
            pred_value = pred_value/gt_value
            gt_value = 1.0
        
        resi = pred_value-gt_value
        
        gt_x.append(word_idx)
        gt_height.append(gt_value)
    
        if resi<=0: # err1, 欠估计
            err1_x.append(word_idx)
            err1_height.append(-resi)
            err1_bottom.append(pred_value)
        else: # err2, 过估计
            err2_x.append(word_idx)
            err2_height.append(resi)
            err2_bottom.append(gt_value)
    
    if len(err1_x) == 0:
        err1_x = [0]
        err1_height = [0]
        err1_bottom = [0]
    if len(err2_x) == 0:
        err2_x = [0]
        err2_height = [0]
        err2_bottom = [0]
        
    error_rate = (sum(err1_height) + sum(err2_height)) / sum(gt_height)
    
    width = 1
    plt.bar(gt_x,gt_height,width=width,
            color='lightgray',label='真实值')
    plt.bar(err1_x,err1_height,bottom=err1_bottom,width=width,
            color='lightgray',hatch='////',edgecolor='C0',linewidth=0.0,label='欠估计')
    plt.bar(err2_x,err2_height,bottom=err2_bottom,width=width,
            color='white',hatch='////',edgecolor='C4',linewidth=0.0,label='过估计')
    
    plt.xlabel('词汇')
    plt.legend('频率')
    plt.xticks(gt_x,gt.keys(),rotation=70,fontsize=5)
        
    if equalized:
        plt.ylim(0.00,1.25)
        plt.yticks(np.arange(0,1.25,0.25))
    else:
        plt.yscale('log')
        
    plt.xlim(-1,len(gt_x))
    
    plt.legend()
    plt.title(title)
    plt.grid(linewidth=0.1)
    
    postfix = 'equalized' if equalized else 'original'
    
    plt.tight_layout()
    plt.savefig(f'./{title} ({postfix}).png',dpi=300)
    plt.close()
    
    return error_rate


if __name__ == '__main__':
    gt = read_dict_from_result_file('./log_mr_v0.txt', topk=100)
    
    title = '真实值 vs CountMin-Sketch'
    pred = read_dict_from_log_file('./log_cm_v2.txt', topk=100)
    
    # title = '真实值 vs Misra-Greis'
    # pred = read_dict_from_result_file('./log_mr_v1.txt', topk=200)
    
    # title = '真实值 vs CountMin-Sketch labeled by Misra-Greis'
    # pred_cm = read_dict_from_log_file('./log_cm_v2.txt', topk=100)
    # pred_mg = read_dict_from_result_file('./log_mr_v1.txt', topk=100)
    # pred = replace_keys(pred_cm,pred_mg)
    
    key_cover_rate = calculate_key_cover_rate(gt,pred)
    error_rate_original = plot_residual(gt,pred,equalized=False,title=title)
    error_rate_equalized = plot_residual(gt,pred,equalized=True,title=title)
    
    with open('log_plot.txt','a') as f:
        f.write(f'Statictics at {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())})\n')
        f.write(f'{title}\n')
        f.write(f'Key cover rate: {key_cover_rate:.6%}\n')
        f.write(f'Error rate (original): {error_rate_original:.6%}\n')
        f.write(f'Error rate (equalized): {error_rate_equalized:.6%}\n')
        f.write('----------\n')