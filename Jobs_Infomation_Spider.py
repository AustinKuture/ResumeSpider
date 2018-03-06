#coding=utf-8

import os
import re
import json
import time
import redis
import socket
import random
import requests
import threading
import pandas as pd
from threading import Thread
from multiprocessing import Process, Queue, Lock


agents = [  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.209.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.208.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.207.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.206.1 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.206.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.204.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.203.2 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.203.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.13 (KHTML, like Gecko) Chrome/4.0.202.0 Safari/525.13. ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.201.1 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.201.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.198.0 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.197.11 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.196.2 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.6 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML,like Gecko) Chrome/3.0.195.27 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.27 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.24 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.21 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.20 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.17 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.10 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.1 Safari/532.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/531.2 (KHTML, like Gecko) Chrome/3.0.191.3 Safari/531.2 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/531.0 (KHTML, like Gecko) Chrome/3.0.191.0 Safari/531.0 ",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/530.8 (KHTML, like Gecko) Chrome/2.0.178.0 Safari/530.8 "]


url = 'http://zhaopin.baidu.com/api/quanzhiasync?'
redis_save = redis.Redis(host='127.0.0.1', port=6379, db=0)


class Spider_Job(object):

    __params = {'rn': 20,
                'tid': 4139,
                'ie': 'utf8',
                'oe': 'utf8',
                'sort_type': 1,
                'query': None,
                'city': None,
                'detailmode': 'close'}

    # 初始化
    def __init__(self, url, jobs='python', areas=None, page_num=1, pro_locks=None, pro_queue=None):

        self.__pro_locks = pro_locks
        self.__pro_queue = pro_queue
        self.__params['query'] = jobs
        self.__params['city'] = areas
        self.__page_total = page_num
        self.__total_list = []
        self.__thd_area = areas
        self.__thd_jobs = jobs
        self.__file_name = jobs+'-'+areas+time.strftime('%Y-%m-%d',time.localtime(time.time()))

        self.url_list = [url+'pn={}'.format(nums*20) for nums in range(self.__page_total)]

    # 开始多线程爬取数据
    def __thd_spider_data(self, url_nums, thd_lock):

        try:

            # 超时时间
            socket.timeout = 7
            thd_request = requests.get(self.url_list[url_nums],
                                       params=self.__params,
                                       headers={'User-Agent':random.choice(agents)})

            data_source = thd_request.content.decode('utf-8')

            data_source = json.loads(data_source)
            data_display = data_source['data']['main']['data']['disp_data']
        except Exception as error:

            print(error)
            return

        try:

            # 遍历数据并获取分类
            for line in data_display:

                source_list = []

                try:

                    source_list.append({'公司名称': line['officialname']})
                except Exception as error:

                    print(error)
                    source_list.append({'公司名称': line['commonname']})

                # 企业类型
                try:

                    com_type = line['ori_employertype']
                except Exception as error:

                    try:

                        source_list.append({'企业类型': line['employertype']})
                    except Exception as error:

                        print(error)
                        source_list.append({'企业类型': '未展示'})
                else:

                    source_list.append({'企业类型': com_type})

                # 工作经验
                try:

                    experience = line['t_experience0']
                except Exception as error:

                    try:
                        source_list.append({'工作经验': line['ori_experience']})
                    except Exception as error:

                        print(error)
                        source_list.append({'工作经验': '未展示'})
                else:

                    source_list.append({'工作经验': experience})

                # 薪资范围
                try:

                    source_list.append({'薪资范围': line['t_salary0']})
                except Exception as error:

                    try:

                        source_list.append({'薪资范围': line['salary']})
                    except Exception as error:

                        print(error)
                        source_list.append({'薪资范围': '未展示'})

                try:

                    source_list.append({'公司规模': line['size']})
                except Exception as errror:

                    try:

                        source_list.append({'公司规模': line['ori_size']})
                    except Exception as error:

                        # print(error)
                        source_list.append({'公司规模': '未展示'})

                try:

                    source_list.append({'经营范围': line['industry']})
                except Exception as error:

                    try:

                        source_list.append({'经营范围': line['jobfirstclass']})
                    except Exception as er:

                        try:
                            source_list.append({'经营范围': line['jobthirdclass']})
                        except Exception as er:

                            print(er)
                            source_list.append({'经营范围': '不限'})

                # 公司网址
                try:
                    source_list.append({'公司网址': line['employerurl']})
                except Exception as error:

                    # print(error)
                    source_list.append({'公司网址': '未展示'})

                source_list.append({'学历要求': line['ori_education']})
                source_list.append({'地区': line['district']})
                source_list.append({'招聘来源': line['source']})
                source_list.append({'职位名称': line['title']})
                source_list.append({'职位描述': line['description_jd']})
                source_list.append({'开始日期': line['startdate']})
                source_list.append({'结束日期': line['enddate']})

                # 字典形式
                dicts = {}
                try:
                    for list_dict in source_list:

                        for name_key, name_value in list_dict.items():

                            try:
                                dicts[name_key] = name_value
                            except Exception as error:

                                print(error)
                                continue
                except Exception as error:

                    print(error)

                # 将数据放到Redis数据库中
                # try:
                #
                #     redis_key = dicts['公司名称']
                #     redis_save.set(redis_key,dicts)
                #
                # except Exception as error:
                #
                #     print(error)
                #     continue

                # 将数据放到队列中
                try:

                    self.__pro_queue.put(dicts)
                except Exception as error:

                    print(error)
                    continue

        except Exception as error:

            print(error)
        else:

            print('------------------------{}-{},第{}页保存完成'.format(self.__thd_jobs,
                                                                      self.__thd_area,
                                                                      url_nums))
        # 为每个线程添加一个随机时间
        # time.sleep(random.randint(0,10)*0.001)

    # 根据任务数开启相应线程
    def run_spider_thd(self):

        thd_list = []
        thd_lock = threading.Lock()

        # 根据单个地区的页码数量创建相应数量的线程
        for thd_st in range(self.__page_total):

            try:

                thd = Thread(target=self.__thd_spider_data,
                             args=(thd_st, thd_lock))
                thd.setDaemon(True)
                thd.start()

            except Exception as error:

                print(error)
                continue
            else:
                thd_list.append(thd)

        # 等待线程结束
        for thd_end in thd_list:

            thd_end.join(timeout=8)

        print('\n{}-{}数据爬取完成，正在处理数据...\n'.format(self.__thd_area,self.__thd_jobs))


# 创建进程处理进行大分类爬取
class Pro_Spider(object):

    def __init__(self, pro_job, area_list, pro_total_num):

        self.__pro_job = pro_job
        self.__pro_area_list = area_list
        self.__pro_total_num = pro_total_num

    # 处理全局爬取的数据
    def __deal_total_data(self, deal_list, file_name, save_queue):

        # 数据分类
        dicts_propertys = ['公司名称', '企业类型', '公司规模',
                           '经营范围', '地区', '招聘来源',
                           '职位名称', '薪资范围', '工作经验',
                           '学历要求', '职位描述', '开始日期',
                           '结束日期', '公司网址']

        # 使用列表推导式进行数据的分类处理
        try:
            data_dicts = {mkey:[line[mkey] for line in deal_list] for mkey in dicts_propertys}
        except Exception as error:

            print(error)
        else:

            # 导出到excel
            job_file_class = 'Jobs_Data'
            if not job_file_class in os.listdir('./'): os.mkdir(job_file_class)

            deal_data = pd.DataFrame.from_dict(data_dicts, orient='index').T
            pd.DataFrame(deal_data).to_excel('./Jobs_Data/{}.xlsx'.format(file_name), sheet_name='one', index=False, header=True)

            print('<数据保存完成>\n')

            if redis_save == 'redis':

                print('正在清洗缓存数据...')
                for line_a in range(10):

                    print('>'*line_a)
                    time.sleep(0.1)

                print('\n缓存清除成功')
                redis_save.flushall()

    # 开始进程的爬取
    def __start_pro_spider(self, jobs, areas, page, pro_locks, pro_queue):

        # 为每个进程创建线程实例化对象
        job_spider = Spider_Job(url=url,
                                jobs=jobs,
                                areas=areas,
                                page_num=page,
                                pro_locks=pro_locks,
                                pro_queue=pro_queue)
        job_spider.run_spider_thd()

    # 创建进程
    def run_pro_spider(self):

        pro_lists = []
        pro_locks = Lock()
        pro_queue = Queue()
        file_area = ''
        # 根据地区列表创建相应进程
        for areaS in self.__pro_area_list:

            try:

                pro = Process(target=self.__start_pro_spider,
                              args=(self.__pro_job,
                                    areaS,
                                    self.__pro_total_num,
                                    pro_locks,
                                    pro_queue))

                pro.start()

            except Exception as error:

                print(error)
                continue
            else:
                pro_lists.append(pro)
                file_area += areaS
                print('------------开始爬取,{}-{}'.format(self.__pro_job, areaS))

        # 等待进程结束
        for pro_end in pro_lists:

            pro_end.join(timeout=int(self.__pro_total_num * 0.6)+1)

        print('\n数据处理完成,正在从缓存中提取数据...\n')

        # 从进程池中取出数据
        pro_main_list = []
        while True:

            try:
                job_dict = pro_queue.get(block=True, timeout=1)
            except Exception as error:

                print(error)
                break
            else:

                pro_main_list.append(job_dict)

        # 读取Redis数据库中的数据
        # key_list = redis_save.keys()
        #
        # for key_line in key_list:
        #
        #     try:
        #         key_line = key_line.decode('utf-8')
        #         value_line = redis_save.get(key_line).decode('utf-8')
        #         value_line = value_line.replace("'", '"').replace('\\','')
        #         value_line = json.loads(value_line)
        #     except Exception as error:
        #
        #         print(error)
        #         continue
        #     else:
        #         pro_main_list.append(value_line)

        print('-----共提取{}条数据，正在保存......\n'.format(len(pro_main_list)))
        fileNames = self.__pro_job + '-' + file_area + time.strftime('%Y-%m-%d-%H:%M', time.localtime(time.time()))

        self.__deal_total_data(pro_main_list, fileNames, 'queue')


if __name__ == '__main__':

    # 职位地区
    area_list = ['深圳','北京','广东','杭州','郑州','上海', '长沙', '天津']

    # 职位名称
    jobs = 'python'

    pro_spiders = Pro_Spider(jobs, area_list, 38)
    pro_spiders.run_pro_spider()

























































