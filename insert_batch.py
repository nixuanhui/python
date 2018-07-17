# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 12:18:57 2018

@author: theo
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Jul 14 09:35:15 2018

@author: theo
"""
from sqlalchemy import create_engine
import pandas as pd
import time
import numpy as np
import pymysql
import math


##批量插入数据库函数
# table_name 表名，column_name 列名，data 带插入数据（dataframe格式），batch 批次，cursor 游标
def insert_goods_and_article_batch(table_name,column_name,data,batch,cursor):
        data=data.astype(str)
        
        
        for i in range(0,math.ceil(data.shape[0]/batch)):
            insert_sql='insert ignore into '+table_name+'('+','.join(column_name)+')'+' values '
            try:
                if i==math.ceil(data.shape[0]/batch)-1:
                    data_tmp=data.loc[i*batch:,:]
                    data_tmp=data_tmp.reset_index(drop=True)
                    for i in range(data_tmp.shape[0]):
                        insert_sql=insert_sql+'('+",".join(str(s) for s in list(data_tmp.loc[i,:]) if s is not None)+"),"
    #                print(insert_sql[:-1])
                    
                    cursor.execute(insert_sql[:-1])
                    break
                else:
                    data_tmp=data.loc[i*batch:(i+1)*batch-1,:]
                    data_tmp=data_tmp.reset_index(drop=True)
    #                print(data_tmp)
                    for j in range(0,data_tmp.shape[0]):
#                        print(data_tmp.iloc[j,:])
                        insert_sql=insert_sql+'('+",".join(list(data_tmp.loc[j,:]))+"),"
#                    print(insert_sql[:-1])
                    
                    cursor.execute(insert_sql[:-1])
                    del data_tmp
            except Exception as err:
                raise(err)
                break
if __name__=='__main__':
    try:
        # 自行填充数据库信息
        engine = create_engine('')
        connect = pymysql.Connect(
                            
                        )

        cursor=connect.cursor()
        
        
        for i in range(1000):
            try:
                sql='select * from cs_user_goods_copy2  where user_id>='+str(user_id)+' limit 50000'
                cs_user_goods=pd.read_sql(sql,con=engine)
                user_id=cs_user_goods.loc[cs_user_goods.shape[0]-1,'user_id']
                print(user_id)
                goods_column_name=['user_id','goods_id','score','city_id','source_id','flag']
                t_1=time.time()
                insert_goods_and_article_batch(table_name='cs_user_goods_info',column_name=goods_column_name,data=cs_user_goods,batch=2000,cursor=cursor)
                print(str(time.strftime('%Y%m%d %H:%M:%S'))+':商品表插入完成,耗时'+str(int(time.time()-t_1)))
                connect.commit()
            except Exception as err:
                
                print(err)
                continue
    finally:
        connect.close()  

