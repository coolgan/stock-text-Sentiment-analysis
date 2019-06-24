import os
import json
import time
import requests
import pandas as pd
from lxml import etree
from bs4 import BeautifulSoup
from multiprocessing import Process,Pool

data=pd.read_csv('创业板.csv')
codes=data['code'].tolist()

def get_stk_comment(code,start_num,end_num,batch_size):
    total_content = []
    for i in range(start_num,end_num):
        url='http://guba.eastmoney.com/list,{code}_{num}.html'.format(code=str(code),num=str(i))
        res = requests.get(url)
        selector = etree.HTML(res.text)
        seeds = selector.xpath('//*[@id="articlelistnew"]/div')[1:-2]
        if seeds!=[]:
            page_content = []
            for seed in seeds:
                dic = {}
                if seed.xpath('./span')[0].xpath('./text()')!=[]:
                    read_count = seed.xpath('./span')[0].xpath('./text()')[0]
                else:
                    read_count = 'None'
                if seed.xpath('./span')[1].xpath('./text()')!=[]:
                    comment_count = seed.xpath('./span')[1].xpath('./text()')[0]
                else:
                    comment_count = 'None'
                if seed.xpath('./span')[2].xpath('./a/text()')!=[]:
                    title = seed.xpath('./span')[2].xpath('./a/text()')[0]
                else:
                    title = 'None'
                if seed.xpath('./span')[3].xpath('./a/text()')!=[]:
                    author = seed.xpath('./span')[3].xpath('./a/text()')[0]
                else:
                    author = 'None'
                if seed.xpath('./span')[-1].xpath('./text()')!=[]:
                    date = seed.xpath('./span')[-1].xpath('./text()')[0]
                else:
                    date = 'None'
                dic = {
                    "read_count":read_count,
                    "comment_count":comment_count,
                    "title":title,
                    "author":author,
                    "date":date
                }
                page_content.append(dic)
            total_content.extend(page_content)
            if (i+1)%batch_size ==0:
                filename = '{code}_{num}.json'.format(code=code,num=str(int((i+1)/batch_size)))

                with open(filename,'w',encoding='utf-8') as js:
                    js.write(json.dumps(total_content,indent=0,ensure_ascii=False))
                total_content = []
            
        else:
            break

def main(code,start_num,end_num,batch_size):
    #新建个股的文件夹
    stk_file='./{}'.format(code)
    os.mkdir(stk_file)
    #进入新文件夹
    os.chdir(stk_file)
    #抓取个股股评
    get_stk_comment(code,start_num=1,end_num=20,batch_size=10)
    #返回上级目录
    abspath=os.path.abspath(os.path.dirname(os.getcwd()))
    os.chdir(abspath)
	
if __name__ == '__main__':
	p=Pool(4)
	p.map(main,codes)

