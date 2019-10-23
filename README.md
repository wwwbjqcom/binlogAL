# my_test

rust语言学习, 并利用来重构了以前python写的部分工具。


# 功能

1. 可直接命令行执行sql语句    
2. 可以对binlog进行解析、过滤 、统计    
3. 可以反转binlog为回滚日志     
4. 可以提取binlog数据为sql语句

## 使用方法

可以使用--help查看所有参数选项(有部分功能还未实现).

## 必配项:

user: 连接mysql的用户名     
password： mysql密码     
host： mysql地址端口     
runtype： 设置工具模式，必须选一个进行设置

## replication模式:

  gtid: 利用gtid进行注册拉取binlog， gtid优先级高于position配置     
  binlogfile： 通过postion同步注册使用的binlog文件     
  position： position位置信息    
  threadid： 想提取某个线程id产生的数据时，配置该选项   
  greptbl： 提取某个表或者某些表产生的数据，格式见--help   
  getsql： 提取为sql语句   
  statiac: 统计每个事务大小   
  直接从mysql拉取binlog只支持对库表信息、连接id信息进行提取，下面可以看到用gtid进行注册的使用方法  
  
	mm:debug xxxxx$ ./mytest -uroot -proot -h 127.0.0.1:3306 --runtype repl --gtid '1886928a-ce21-11e9-bee2-50edb3ba887e:1-11' RotateLog { binlog_file: "bin.000001" }  
	GtidEvent     gtid:1886928a-ce21-11e9-bee2-50edb3ba887e, gno_id:12, last_committed:2818, sequence_number:3072  
	QueryEvent    thread_id:1511, database:xz_test, command:BEGIN  
	TableMap      database_name:xz_test, table_name:t8  
	ROW_VALUE  
	 id: 1, id1: 3, a: b, b: 2, c: 0x36333633,XidEvent      xid:3089  
	GtidEvent     gtid:1886928a-ce21-11e9-bee2-50edb3ba887e, gno_id:13, last_committed:3074, sequence_number:3328  
	QueryEvent    thread_id:1519, database:xz_test, command:BEGIN  
	TableMap      database_name:xz_test, table_name:t8  
	ROW_VALUE  
	 id: 1, id1: 4, a: bb, b: 2, c: 0x36333634,
	XidEvent      xid:3114  
	GtidEvent     gtid:1886928a-ce21-11e9-bee2-50edb3ba887e, gno_id:14, last_committed:3330, sequence_number:3584  
	QueryEvent    thread_id:1511, database:xz_test, command:BEGIN  
	TableMap      database_name:xz_test, table_name:t6  
	ROW_VALUE  
	 id: 1, a: a, b: 2, c: 1, d: 0x3742,e: abc, 
	XidEvent      xid:3116  
	mm:debug xxxxx$ ./mytest -uroot -proot -h 127.0.0.1:3306 --runtype repl --gtid '1886928a-ce21-11e9-bee2-50edb3ba887e:1-11' --threadid 1511 --greptbl '{"xz_test":["t8"]}' --getsql  
	-- GTID: 1886928a-ce21-11e9-bee2-50edb3ba887e:12  
	use xz_test;  
	BEGIN;  
	-- Insert Row Value  
	INSERT INTO xz_test.t8(id,id1,a,b,c) VALUES(1,3,'b',2,0x36333633);  
	COMMIT;

## 读取binlog文件:

file: 指定binlog文件   
startposition： 从那个postion开始读取   
stopposition： 停止位置   
startdatetime： 提取时间范围的binlog时，开始时间   
stopdatetime： 停止时间   
getsql： 提取为sql语句   
threadid: 提取某个线程id产生的数据   
greptbl: 提取某些表产生的数据   
gtid: 在该模式下配置gtid，则为提取对于gtid的数据   
statiac: 统计每个事务大小    
配置项可以多种搭配方式，比如我想统计某个positon范围中某个thread_id产生的某个表的信息  

	mm:debug xxxxx$ ./mytest -uroot -proot -h 127.0.0.1:3306 --runtype file --file 'bin.000001' --startposition 3636 --threadid 1511 --greptbl '{"xz_test":"all"}'  
	从binlog文件提取数据  
	GtidEvent     gtid:1886928a-ce21-11e9-bee2-50edb3ba887e, gno_id:12, last_committed:2818, sequence_number:3072  
	QueryEvent    thread_id:1511, database:xz_test, command:BEGIN  
	TableMap      database_name:xz_test, table_name:t8  
	ROW_VALUE  
	 id: 1, id1: 3, a: b, b: 2, c: 0x36333633,XidEvent      xid:3089  
	GtidEvent     gtid:1886928a-ce21-11e9-bee2-50edb3ba887e, gno_id:14, last_committed:3330, sequence_number:3584  
	QueryEvent    thread_id:1511, database:xz_test, command:BEGIN  
	TableMap      database_name:xz_test, table_name:t6  
	ROW_VALUE  
	 id: 1, a: a, b: 2, c: 1, d: 0x3742,e: abc, 
	XidEvent      xid:3116

## 回滚：

1、只能从binlog文件获取  
2、默认以1G做为单个文件最大值,可以通过配置项自行修改  
3、事务顺序倒叙生成  
4、可直接使用mysqlbinlog进行操作恢复也可以直接用工具提取sql恢复  
5、提取配置参数同读取binlog文件方式，全部通用  
6、支持8.0及以下版本


## 回滚使用方法：
参数可以同文件读取一样任意搭配，比如我想把上面文件读取打印的binlog进行回滚只需要加个--rollback就行, 执行完成会在当前目录产生以rollback开头的日志文件

	mm:debug xxxxx$ ./mytest -uroot -proot -h 127.0.0.1:3306 --runtype file --file 'bin.000001' --startposition 3636 --threadid 1511 --greptbl '{"xz_test":"all"}' --rollback  
	从binlog文件提取数据  
	failed to fill whole buffer  
	mm:debug xxxxx$ ./mytest -uroot -proot -h 127.0.0.1:3306 --runtype file --file 'rollback-1.log' --getsql 从binlog文件提取数据  
	-- GTID: 1886928a-ce21-11e9-bee2-50edb3ba887e:14  
	use xz_test;  
	BEGIN;  
	-- Insert Row Value  
	INSERT INTO xz_test.t6(id,a,b,c,d,e) VALUES(1,'a',2,1,0x3742,'abc');  
	COMMIT;  
	-- GTID: 1886928a-ce21-11e9-bee2-50edb3ba887e:12  
	use xz_test;  
	BEGIN;  
	-- Delete Row Value  
	DELETE FROM xz_test.t8  WHERE id=1 AND id1=3;  
	COMMIT;
	
可以看到已经把数据反转为对应的回滚语句， 可以直接使用mysqlbinlog进行操作，也可以直复制提取的sql进行执行，如果使用mysqlbinlog操作方式如下

	bin/mysqlbinlog rollback-1.log --skip-gtids | bin/mysql -uroot -proot -h 127.0.0.1
## 语句执行：

连接mysql未使用开源框架，是直接通过socket连接实现mysql协议的，所以这里弄了一个语句执行的模式来验证，通过-c参数直接指定sql语句, 可以使用-D进行默认库指定，如果不指定则在sql中需要写明

	./mytest -uroot -proot -h 127.0.0.1:3306 -D information_schema -c 'select * from tables'
