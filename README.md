# kotlin-dbms
Implementing a database management system using kotlin.  
With reference to [Database Design and Implementation](https://www.amazon.com/dp/3030338355/).

# Demo

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
Using Intellij  
Set up data


Run Server
