'''
MIT License

Copyright (c) 2019 OpenSRCi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
''' 

import requests
import pymysql, os, json
from datetime import datetime

#debug
import sys

# do validation and checks before insert
def val_str(val):
   if val != None:
        if type(val) is int:
            return str(val).encode('utf-8')
        else:
            return val

def retrieve( reqHeader  ):
    headers={   
                'Content-Type':'application/json',
                'authorization':'Bearer '+ reqHeader["api_token"]
    }
    
    data = { "expend":"", "access_token" : reqHeader["api_token"] }
    
    try:
        myResponse = requests.get( reqHeader["url"], data=data, headers= headers )
        
        myResponse.raise_for_status()
        # Loading the response data into a dict variable
        # json.loads takes in only binary or string variables so using content to fetch binary content
        # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
    
        json_obj  = json.loads( myResponse.text )
    
        # connect to MySQL
        connection = pymysql.connect( host=reqHeader["db_host"], user=reqHeader["db_user"], passwd=reqHeader["db_pwd"], db=reqHeader["db"] )
        cursor = connection.cursor()
        
        
        # parse json data to SQL insert
        sql_data = []
    
        for i,item in enumerate(json_obj["elements"]):
            val_fmt = []
            col_fmt = []
            v = []
            for j,field in enumerate(reqHeader["db_cols"]):
                try:
                    col = str( reqHeader["db_cols"][field] )
                    col_type = reqHeader["db_col_type"].get(field)
                    if( col != '' ):
                    #is array
                        sub_col = reqHeader["db_cols"].get(field)
                        for k,sf in enumerate(sub_col):
                            col_type = reqHeader["db_col_type"][field].get(sf)
                            val_fmt.append('%s')
                            col_fmt.append('`'+ str(field) + '_' + str(sf) + '`')
                            try:
                                f = item[field][sf]
                            except:
                                f = ""
 
                            if ( 'DATETIME' in col_type ):
                                f = datetime.utcfromtimestamp(f/1000).strftime('%Y-%m-%d %H:%M:%S')

                            v.append(f)                            
                           
                    else: 
                        val_fmt.append('%s')
                        col_fmt.append('`'+ str(field)+'`')
                        try:
                            f = item.get(field)
                        except:
                            f = ""

                        if ( 'DATETIME' in col_type ):
                            f = datetime.utcfromtimestamp(f/1000).strftime('%Y-%m-%d %H:%M:%S')
                        v.append(f)                            
                    
                except:
                    pass

            sql_data.append (v)
            
            cols = ','.join(col_fmt)
            vals = ','.join(val_fmt)
    
            
        try:
            sql_exec = 'INSERT INTO ' + str( reqHeader["db_table"] ) + ' (' + cols + ') VALUES (' + vals + ') '
            cursor.executemany( sql_exec , sql_data  )
            connection.commit()
            print("insert successfully")
        
        except pymysql.Error as e:
            print("mySQL Error %d: %s" % (e.args[0], e.args[1]))
    
    except  :
        print("Error:" , sys.exc_info()[0])
    else:
        connection.close()
        return("Successful")
    

