# Phouse easy work with clickhouse 

# for install use
```python
pip3 install git+git://github.com/bedretdinov/phouse.git

```

# Examples
```python

from phouse.phouse import Phouse 


# The connection will be repeat if get failed. It is need for stability your ETL scripts.
# The script will try until get connection. try with interval 60 sec
pd = Phouse.getConnection({
    'host':'0.0.0.0',
    'user':'default',
    'password':'',
    'database':'owox'
})
  
# return pandas dataframe
df = pd.clickhouse_query(''' SELECT database, name FROM system.tables ''')

# !!!! And very important things this data frame must have date column else you get error
df['date'] = pd.to_datetime('today')

# insert to the table
df.append(table='new_table')

# truncate table and insert
df.write(table='new_table')


```
 
