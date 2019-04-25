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
import config as cfg
import pymysql, os, json

# do validation and checks before insert
def val_str(val):
   if val != None:
        if type(val) is int:
            return str(val).encode('utf-8')
        else:
            return val

headers={   
            'Content-Type':'application/json',
            'authorization':'Bearer '+ cfg.API_TOKEN
}

data = { "expend":"", "access_token" : cfg.API_TOKEN  }

try:
    myResponse = requests.get( cfg.url, data=data, headers= headers )
    
    myResponse.raise_for_status()
    # Loading the response data into a dict variable
    # json.loads takes in only binary or string variables so using content to fetch binary content
    # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)

    json_obj  = json.loads( myResponse.text )

# connect to MySQL
    connection = pymysql.connect( host=cfg.DB_HOST, user=cfg.DB_USER, passwd=cfg.DB_PWD, db=cfg.DB )
    cursor = connection.cursor()
    
    
# parse json data to SQL insert
    sql_data = []

    for i, item in enumerate(json_obj["elements"]):
        val_fmt = []
        col_fmt = []
        v = []
        for j,field in enumerate(cfg.TRX_COL):
            if( cfg.TRX_COL[field]):
            #is array
                for k,sf in enumerate(cfg.TRX_COL[field]):
                    val_fmt.append('%s')
                    col_fmt.append('`'+ str(field) + '_' + str(sf) + '`')
                    v.append(item[field][sf])

            else:    
                val_fmt.append('%s')
                col_fmt.append('`'+field+'`')
                try:
                    v.append(item[field])
                except:
                    v.append(0)

        sql_data.append (v)
        
        cols = ','.join(col_fmt)
        vals = ','.join(val_fmt)

        
    try:
        sql_exec = 'INSERT INTO ' + str(cfg.TRX_TABLE) + ' (' + cols + ') VALUES (' + vals + ') '
        cursor.executemany( sql_exec , sql_data  )
        connection.commit()
        print("insert successfully")
    
    except pymysql.Error as e:
        print("mySQL Error %d: %s" % (e.args[0], e.args[1]))

except requests.exceptions.HTTPError as errh:
    print ("Http Error:",errh)
except requests.exceptions.ConnectionError as errc:
    print ("Error Connecting:",errc)
except requests.exceptions.Timeout as errt:
    print ("Timeout Error:",errt)
except requests.exceptions.RequestException as err:
    print ("OOps: Something Else",err)
finally:
    connection.close()
    print("the end.")
    

