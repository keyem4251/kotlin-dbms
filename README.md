# kotlin-dbms
Implementing a database management system using kotlin.  
With reference to [Database Design and Implementation](https://www.amazon.com/dp/3030338355/).

# Content

Simpledb is a database engine that can handle a subset of SQL.
This database use only Int and String Type.  
And this database can handle following algebra.

- select F1, F2 from T1 where F2 = Value
- select F1, F2 from T1, T2 where F3=F4
- insert into T(F1, F2, F3) values ('a', 'b', 'c')
- delete from T where F1=F2
- update T set F1='a' where F1=F2
- create table T(F1 int, F2 varchar(9))
- create view V as select F1, F2 from T1, T2 where F3=F4
- create index I on T(F)

Don't implement Remote JDBC interface in [Database Design and Implementation](https://www.amazon.com/dp/3030338355/).  
Only implement Embedded JDBC interface.

# Getting Started
Using IntelliJ  
## Set up data
https://user-images.githubusercontent.com/53423344/160232120-e0013c3e-f3c1-41c3-a733-77cbaf981689.mp4

## Run Server
https://user-images.githubusercontent.com/53423344/160232133-1e9ac9e4-c1dc-40f4-a427-9f0aeae682fd.mp4
