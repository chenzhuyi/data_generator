# data_generator

1. By default, the source databse is Oracle. If you want to change the data source, you could add relevant driver into dependency in pom.xml
2. Use Maven to build project
3. Edit src/main/resources/application.properties. The first part is connection properties, the second part is runtime configuration
  
### Runtime configuration
-- How many threads run parallelly
  
#### runtime.parallel.threads=1   
        
-- Table name
#### runtime.tablename=LR_USER.TESTTABLE             

-- If truncate table at first
#### runtime.delete.onstart=true

-- Delete threshold. 0 means there's no delete operation
#### runtime.delete.threshold=0

-- Max rows are inserted per thread
#### runtime.max.insert.count.perthread=100000

-- Max transaction size
#### runtime.max.transaction.size=1000

-- Max rows are updated per thread. 0 means there's no update operation
#### runtime.max.update.count.perthread=500

-- You could specify the column values via column1=val1,column2=val2,...
#### runtime.specify.values=ADDRESS1=New York

-- Max tried round if fail to do DML in batch 
#### runtime.max.try.round=10
        
