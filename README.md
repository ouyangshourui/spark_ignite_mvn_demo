# spark_ignite_mvn_demo
spark2.3.0 read  data  from ignite1.6.0 and write data to ignite1.6.0
# ignite create table 
sqlline.sh --verbose=true -u jdbc:ignite:thin://bigdata-ignite-04

```sql 
CREATE TABLE event_10001_user(                                                                                             
    _ftenancy_id bigint,                                                                                                                      
    _fid varchar,                                                                                                                             
    fsbabyage bigint,                                                                                                                         
    fsbirthday varchar,                                                                                                                       
    fuserlevel bigint,                                                                                                                        
    fbabynum bigint,                                                                                                                          
    fbbirthday varchar,                                                                                                                       
    fbirthday varchar,                                                                                                                        
    fcreatordepartment varchar,                                                                                                               
    fearliestregtime varchar,                                                                                                                 
    fmobile varchar,                                                                                                                          
    fsex bigint,                                                                                                                              
    fssex bigint,                                                                                                                             
    ftruename varchar,                                                                                                                        
    fbbabyage bigint,                                                                                                                         
    fbsex bigint,                                                                                                                             
    fcityname varchar,                                                                                                                        
    fcommunity varchar,                                                                                                                       
    fdistrictname varchar,                                                                                                                    
    fgestationage bigint,                                                                                                                     
    fnickname varchar,                                                                                                                        
    fpromoteactive varchar,                                                                                                                   
    fprovincename varchar,                                                                                                                    
    fcreator varchar,                                                                                                                         
    fbirthday_next varchar,                                                                                                                   
    fmemberlevel bigint,
    PRIMARY KEY (_ftenancy_id, _fid)                                                                                                                        
 )WITH "template=partitioned, backups=3, CACHE_NAME=event_10001_user";

```
