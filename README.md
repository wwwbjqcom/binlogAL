# my_test

rust语言学习, 并利用来重构了以前python写的部分工具。



# 功能

 1. 可直接命令行执行sql语句
 2. 可以对binlog进行解析、过滤 、统计
 3. 可以反转binlog为回滚日志 
 4. 可以提取binlog数据为sql语句 


## 使用方法：
可以使用--help查看所有参数选项(有部分功能还未实现)

### 必配项: 
	user: 连接mysql的用户名 
	password： mysql密码 
	host： mysql地址端口 
	runtype： 设置工具模式，必须选一个进行设置

### replication模式:  
	gtid: 利用gtid进行注册拉取binlog， gtid优先级高于position配置 
	binlogfile： 通过postion同步注册使用的binlog文件 
	position： position位置信息
	threadid： 想提取某个线程id产生的数据时，配置该选项
	greptbl： 提取某个表或者某些表产生的数据，格式见--help
	getsql： 提取为sql语句
	statiac: 统计每个事务大小
	
### 读取binlog文件:
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
	
### 回滚：
	1、只能从binlog文件获取，直接把回滚数据写入到rollback.log中，同binlog日志格式
	2、默认以1G做为单个文件最大值
	3、事务顺序倒叙生成
	4、可直接使用mysqlbinlog进行操作恢复也可以直接用工具提取sql恢复

### 语句执行：
	通过-c参数直接指定sql语句, 可以使用-D进行默认库指定，如果不指定则在sql中需要写明
	./mytest -uroot -proot -h 127.0.0.1:3306 -D information_schema -c 'select * from tables'



