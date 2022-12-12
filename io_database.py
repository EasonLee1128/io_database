import pymysql
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import time
import json

class database():
    def __init__(self,host,user,password,database,port):  #資料庫連線設定
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.port=port
        self.conn = pymysql.connect(host=self.host,user=self.user, password=self.password, database=self.database,port=self.port)
        self.cursor = self.conn.cursor()  
    def __del__(self):
        self.conn.close()
               
    def readData(self,tableName,columnList):#讀取資料表,欄位資料
        columnString=''
        for i in columnList:    
            columnString = f'{columnString},'+ i        
        columnString = columnString[1:]    
        star = columnString           
        sqlstring = f'SELECT {star} FROM {tableName}'     
        #self.cursor = self.conn.cursor() 
        self.cursor.execute(sqlstring)   
        data = self.cursor.fetchall()
        data = DataFrame(data)
        #self.conn.close()   
        return data 
    def data_toDictString(self,columnDic): #transform 把字典轉成字串
        dictListString=''
        for key  in columnDic:
            dictListString = dictListString+f',"{key}":"{columnDic[key]}"'           
        dictListString = dictListString[1:] 
        dictListString= '{'+dictListString+'}'        
        columnDic = json.loads(dictListString)    
        return columnDic   
    def insertData(self,tableName,columnDic): #增加資料
        columnDic= self.data_toDictString(columnDic) 
        columnList = list(columnDic.keys())
        columnValue = list(columnDic.values())     
        columnString=''
        for i in columnList:    
            columnString = f'{columnString},'+ i        
        columnString = columnString[1:]        
        columnValueString=''
        for i in columnValue:    
            columnValueString = f'{(columnValueString)}","'+ i                   
        columnValueString = columnValueString[2:]+'"'
        columnValueString = columnValueString.replace("'","~")
        columnValueString = columnValueString.replace('"',"'")
        columnValueString = columnValueString.replace("~",'"') 
        sqlstring =f'INSERT INTO  {tableName} ( {columnString}) VALUES ( {columnValueString})' 
        self.cursor.execute(sqlstring)
        self.conn.commit()
        #self.conn.close() 
        return True
    def updateData(self,tableName,columnDic):#更新資料
        columnDic=self.data_toDictString(columnDic)        
        id = columnDic['id']      
        columnDicitems = columnDic.items()
        print(columnDic.items())        
        KeyValuestring=''
        for i in columnDicitems:
            print(i)    
            KeyValuestring=KeyValuestring+f',{i[0]}="{i[1]}"'     
        KeyValuestring=  KeyValuestring[1:]           
        sqlstring = f'UPDATE {tableName} set {KeyValuestring}  where id = {id}' #OKOK
        self.cursor.execute(sqlstring)
        self.cursor.commit()
        #self.cursor.close()
        return True
    def deleteData(self,tableName):          
        sqlstring = f'delete FROM {tableName}' #OKOK           
        self.cursor.execute(sqlstring)
        data = self.cursor.fetchall()        
        self.conn.commit()
        #self.conn.close()   
            
        return True
    
    def truncateTable(self,tableName):
        sqlstring = f'TRUNCATE TABLE {tableName}' #清空資料表          
        self.cursor.execute(sqlstring)
        data = self.cursor.fetchall()        
        self.conn.commit()
        
    def table_columnName(self,tableName): #回傳資料表個欄位名稱
        #tableName='aff_agent_statics_by_month'
        sqlstring=f'SHOW COLUMNS FROM {tableName};' 
        self.cursor.execute(sqlstring) 
        self.conn.commit()
        data = self.cursor.fetchall()
        self.conn.close() 
        dataDF = DataFrame(data)
        return dataDF[0]
    
    def table_columnNameCHT(self,tableName): #回傳資料表個欄位名稱
        #tableName='aff_agent_statics_by_month'
        sqlstring=f'SHOW COLUMNS FROM {tableName};' 
        self.cursor.execute(sqlstring) 
        self.conn.commit()
        data = self.cursor.fetchall()
        self.conn.close() 
        dataDF = DataFrame(data)
        return dataDF[7]
    

if __name__ == '__main__':
    pass


