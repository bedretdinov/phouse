import pandas as pd
from clickhouse_driver import Client 
import sys
import time

@pd.api.extensions.register_dataframe_accessor("clickhouse")
class Clickhouse:
    
    DATE_COLUMN = None
    COLUMN_LIST = None
    DATA_FRAME  = None
    MAPPING = {
        'int8'         :"Int8 default CAST(0, 'Int8')",
        'int16'         :"Int16 default CAST(0, 'Int16')",
        'int32'         :"Int32 default CAST(0, 'Int32')",
        'int64'         :"Int64 default CAST(0, 'Int64')",
        'float32'       :"Float32 default CAST(0, 'Float32')",
        'float64'       :"Float64 default CAST(0, 'Float64')",
        'object'        :'Nullable(String)',
        'datetime64[ns]':'DateTime default today()', 
    }
    
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj
 
    @classmethod
    def _validate(cls,obj):
        cls.DATA_FRAME = obj  
        cls.COLUMN_LIST = []
        for c in obj.columns:  
                if(str(obj[c].dtype)=='datetime64[ns]'):
                    cls.DATE_COLUMN = c
                cls.COLUMN_LIST.append("\"{0}\" {1}".format(c,cls.MAPPING[str(obj[c].dtype)])  )
            
    
    def createTable(self, table):
        CREATE_SQL = '''
            create table {table_name}
            (
               {columns} 
            )
              engine = MergeTree() PARTITION BY toYYYYMM({date_column}) 
              ORDER BY {date_column} SETTINGS index_granularity = 8192; 
        '''.format(**{
            'table_name'     :table,
            'columns'        : ",\n".join(self.COLUMN_LIST),
            'date_column'    :self.DATE_COLUMN 
        }) 
        
        try: 
            pd.clickhouse_client.execute(CREATE_SQL, with_column_types=True)
        except:
            pass
     
    def write(self, table):  
        while(True): 
            try:
                self.createTable(table) 
                pd.clickhouse_client.execute("TRUNCATE TABLE {}".format(table), with_column_types=True) 
                pd.clickhouse_client.execute(
                     'INSERT INTO {0} ({1}) VALUES'.format( table , ','.join(
                      [ "\"{}\"".format(x) for x in Clickhouse.DATA_FRAME.columns] 
                     )),
                      self.DATA_FRAME.to_dict('records')
                    )  
                break
            except:
                print("error query {0} . try again after {1} second".format(sys.exc_info()[0],60))
                time.sleep(60)
            
        return None

    def append(self, table):  
        
        while(True):
            try:
                self.createTable(table) 
                pd.clickhouse_client.execute(
                 'INSERT INTO {0} ({1}) VALUES'.format( table , ','.join(
                  [ "\"{}\"".format(x) for x in self.DATA_FRAME.columns] 
                 )),
                  self.DATA_FRAME.to_dict('records')
                ) 
                break
            except: 
                print("error query {0} . try again after {1} second".format(sys.exc_info()[0],60))
                time.sleep(60)
            
        return None 

    
    
class Phouse():
    
    @classmethod
    def getConnection(cls,options, ):   
            import pandas as pd
            pd.clickhouse_client = Client(options['host'], 
                user=options['user'], 
                password=options['password'],
                secure=False,
                verify=False,
                database=options['database'],  
                connect_timeout=60*20
            )
            
            def clickhouse_query(sql, repeat_interval=60):  
                while(True): 
                    try:
                        data = pd.clickhouse_client.execute(sql, with_column_types=True)  
                        break
                    except: 
                        print("error query {0} . try again after {1} second".format(sys.exc_info()[0],repeat_interval))
                        time.sleep(repeat_interval)
                
                return pd.DataFrame(data[0], columns=[ c[0] for c in data[1] ])
            

            pd.clickhouse_query = clickhouse_query  
            
            return pd