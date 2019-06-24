import json
import os
import datetime
import pandas as pd

##======第一部分：给股评标注年份=====##
#由于东方财富股吧没有显示股评发布的年份，需要手动标注发布年份
#具体思路：遍历每条股评，如果下一条股评发布的月份大于当前股评发布月份，当前年份减一（e.g.2019-01-01 & 2018-12-31）
def df_time_processing(total):
    #把合并以后的json文件转为dataframe
    df=pd.DataFrame(total)
    df=df[20:]#剔除前二十条股评（官方置顶股评，不算个人情绪）
    
    #提取股评发布的月份
    f=lambda x:float(x.split('-')[0])
    xx=df['date'].apply(f).iloc[1:]#股评当前月份
    xx2=df['date'].apply(f).shift().iloc[1:]#上一条股评发布月份
    temp=[];date=[]
    flag=2019#比较宽松的假设：假定第一条股评都在今年（2019）发布
    
    #示性函数，如果上一条股评发布的月份大于当前股评发布月份，就假设年份减1
    xx4=(xx2-xx).tolist()
    for i in range(len(xx4)):
        if xx4[i]<0:
            flag=flag-1
        temp.append(flag)#存储每条股评的发布年份
    
    #map函数，在df['date']前加上年份
    itera=list(map(lambda x,y:str(x)+'-'+y,temp,df['date']))
    itera.append(str(temp[-1])+'-'+df['date'].tolist()[-1])
    df['Date']=itera
    return df

#主循环：处理所有股票的股评
path='D:\pyhon\data'
#首先合并个股的所有股评json文件
filelist=os.listdir(path)
for file in filelist[1:]:
    #获取当前股票文件夹内的文件名
    sub_filelist=os.listdir(path+'\\'+file)
    total=[]
    if len(sub_filelist)>1:
        for i in range(len(sub_filelist)-1):
            sub_filename=path+'\\'+file+'\\'+file+'_{}.json'.format(str(i+1))
            with open(sub_filename,'r',encoding='utf-8') as js:
                js_dict=json.load(js)
                total.extend(js_dict)
    else:
        #如果文件夹为空则打印如下内容
        print('length of {} is zero.'.format(str(file)))
        pass
    #对合并以后的股评文件用df_time_processing函数处理
    try:
        df=df_time_processing(total)
        df.to_csv(path+'\\'+file+'\\'+file+'.csv',encoding='utf_8_sig')
    except Exception:
        print(file,filelist.index(file))
		
##====第二部分：把股评的json文件转为csv文件
path='D:\python\data'
filelist=os.listdir(path)
for file in filelist[1:]:
    try:
        df=pd.read_csv(path+'\\'+file+'\\'+file+'.csv')
        f=lambda x:x.split(' ')[0]
        date=df['Date'].apply(f)
        df['date']=date
        del df['Date']
        del df['Unnamed: 0']
        df.to_csv(path+'\\'+file+'\\'+file+'real.csv',encoding='utf_8_sig')
    except Exception:
        print(file,filelist.index(file))
