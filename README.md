# plslq_memcached
plsql client for memcached

I wrote & test this plsql-package for memcached 1.5.12 and oracle 11.2.0.3 
There is sql-script test_memcached.sql/ the script was used as a test-tool;

It communicates with memcached-instance(s) by [ascii protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

At the current moment it supports set,get,gat,delete commands
