/*
@author: xiao cai niao
@datetime: 2019/9/17
*/

pub mod readvalue;
pub mod meta;
pub mod stdout;
pub mod io;
pub mod replication;
use std::str;
use std::process;


use structopt::StructOpt;
use std::net::TcpStream;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
pub struct Opt {
    #[structopt(long = "runtype",help = "程序运行模式, [repl: 模拟slave获取binlog数据, mha: 高可用管理模式, monitor: 实时监控mysql运行状态, command: 执行sql语句, file: 从binlog文件获取数据, databus: 同步数据到其他db]")]
    pub runtype: Option<String>,

    #[structopt(short = "u", long = "user",help = "mysql用户名")]
    pub user: Option<String>,

    #[structopt(short = "p", long = "password",help = "mysql密码")]
    pub password: Option<String>,

    #[structopt(short = "h",long = "host", help="ip地址加端口, ip:port 例如127.0.0.1:3306")]
    pub host: Option<String>,

    #[structopt(short = "D",long = "database", help="执行sql语句时连接的默认数据库")]
    pub database: Option<String>,

    #[structopt(short = "c",long = "command", help="sql语句")]
    pub command: Option<String>,

    #[structopt(short = "f",long = "file", help="binlog文件，用于文件读取与配置文件的指定")]
    pub file: Option<String>,

    #[structopt(long = "binlogfile", help="binlog文件名，用于replicaton注册同步")]
    pub binlogfile: Option<String>,

    #[structopt(long = "position", help="注册replication使用的pos")]
    pub position: Option<String>,

    #[structopt(long = "gtid", help="作用于gtid同步模式使用的gtid或者从文件读取时需要过滤的gtid")]
    pub gtid: Option<String>,

    #[structopt(long = "serverid", help="注册用的server_id，不能与已经存在的同步线程重复, 默认为133")]
    pub serverid: Option<String>,

    #[structopt(long = "getsql", help="从binlog文件或者数据流中提取sql语句")]
    pub getsql: bool,

    #[structopt(long = "rollback", help="反转binlog数据为回滚数据，写入二进制文件")]
    pub rollback: bool,

    #[structopt(long = "statisc", help="统计row_event数据大小")]
    pub statisc: bool,

    #[structopt(long = "startposition", help="从binlog文件提取数据时的position起始位置")]
    pub startposition: Option<String>,

    #[structopt(long = "stopposition", help="从binlog文件提取数据时停止的位置")]
    pub stopposition: Option<String>,

    #[structopt(long = "startdatetime", help="读取binlog文件数据的时间范围(时间戳格式)")]
    pub startdatetime: Option<String>,

    #[structopt(long = "stopdatetime", help="读取binlog文件数据的时间范围(时间戳格式)")]
    pub stopdatetime: Option<String>,

    #[structopt(long = "threadid", help="过滤出某个线程id所产生的binlog数据")]
    pub threadid: Option<String>,

    #[structopt(long = "greptbl", help="过滤出某个表所产生的binlog数据，格式为{'db1':['tb1','tb2'],'db2':[]...}}, 如果要提取一个库的所有表格式为{'db1':'all',....}")]
    pub greptbl: Option<String>,

    #[structopt(long = "mhaserver", help="做为高可用管理服务端启动, 开启选项需指定配置文件")]
    pub mhaserver: bool,

    #[structopt(long = "mhaclient", help="做为高可用客户端启动, 与mysql实例在同一节点上, 开启需指定配置文件")]
    pub mhaclient: bool,

    #[structopt(long = "rfilesize", help="单个回滚日志文件大小, 可以不用设置, 默认1G, 设置值是以字节为单位")]
    pub rfilesize: Option<String>,

}

#[derive(Debug, Clone)]
pub struct Config {
    pub runtype: String,
    pub host_info: String,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub program_name: String,
    pub command: String,
    pub file: String,
    pub binlogfile: String,
    pub position: String,
    pub gtid: String,
    pub serverid: String,
    pub getsql: bool,
    pub rollback: bool,
    pub mhaserver: bool,
    pub mhaclient: bool,
    pub statisc: bool,
    pub startposition: String,
    pub stopposition: String,
    pub startdatetime: String,
    pub stopdatetime: String,
    pub threadid: String,
    pub greptbl: String,
    pub rfilesize: String,
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut host_info = String::from("");
        let mut user_name = String::from("");
        let mut password = String::from("");
        let mut database = String::from("");
        let mut command = String::from("");
        let mut file = String::from("");
        let mut binlogfile = String::from("");
        let mut position = String::from("");
        let mut gtid = String::from("");
        let mut runtype = String::from("");
        let mut serverid = String::from("");
        let getsql = args.getsql;
        let rollback = args.rollback;
        let mhaserver = args.mhaserver;
        let mhaclient = args.mhaclient;
        let statisc = args.statisc;
        let mut startposition = String::from("");
        let mut stopposition = String::from("");
        let mut startdatetime = String::from("");
        let mut stopdatetime = String::from("");
        let mut threadid = String::from("");
        let mut greptbl = String::from("");
        let mut rfilesize = String::from("");

        match args.rfilesize {
            None => {},
            Some(t) => rfilesize = t,
        }

        match args.startposition {
            None => {},
            Some(t) => startposition = t,
        }

        match args.stopposition {
            None => {},
            Some(t) => stopposition = t,
        }
        match args.startdatetime {
            None => {},
            Some(t) => startdatetime = t,
        }
        match args.stopdatetime {
            None => {},
            Some(t) => stopdatetime = t,
        }
        match args.threadid {
            None => {},
            Some(t) => threadid = t,
        }
        match args.greptbl {
            None => {},
            Some(t) => greptbl = t,
        }

        match args.user {
            None => {
                return Err("user 不能为空！！");
            },
            Some(t) => user_name = t,
        }

        match args.host {
            None => {
                return Err("host 不能为空！！");
            },
            Some(t) => host_info = t,
        }

        match args.password {
            None => {
                return Err("password 不能为空！！")
            },
            Some(t) => password = t,
        }

        match args.database {
            None => (),
            Some(t) => database = t,
        }

        match args.command {
            None => (),
            Some(t) => command = t,
        }

        match args.file {
            None => (),
            Some(t) => file = t,
        }

        match args.binlogfile {
            None => (),
            Some(t) => binlogfile = t,
        }

        match args.position {
            None => (),
            Some(t) => position = t
        }

        match args.gtid {
            None => (),
            Some(t) => gtid = t,
        }

        match args.runtype {
            None => (),
            Some(t) => runtype = t,
        }

        match args.serverid {
            None => (serverid = 133.to_string()),
            Some(t) => serverid = t,
        }


        Ok(Config { program_name:String::from("rust_test"),statisc,rfilesize,
            host_info, user_name ,getsql,rollback,startposition,stopposition,
            password, database,serverid,startdatetime,stopdatetime,threadid,greptbl,
            command,file,binlogfile,position,gtid,runtype,mhaserver,mhaclient})
    }
}

pub fn startop(config: &Config) {
    if config.runtype == String::from("command"){
        let mut conn = create_conn(config);
        let values = io::command::execute(&mut conn,&config.command);
        for row in values.iter(){
            println!("{:#?}",row);
        }
    }else if config.runtype == String::from("repl") {
        let mut conn = create_conn(config);
        replication::repl_register(&mut conn,&config);
    }else if config.runtype == String::from("file") {
        println!("从binlog文件提取数据");
        let mut conn = create_conn(config);
        replication::repl_register(&mut conn,config);

    }else {
        println!("无效的执行参数runtype: {}, --help提供参考",config.runtype);
    }



//    use std::{thread, time};
//    let ten_millis = time::Duration::from_secs(100);
//    thread::sleep(ten_millis);

}


fn create_conn(config: &Config) -> TcpStream {
    let conn = io::connection::create_mysql_conn(config).unwrap_or_else(|err|{
        println!("创建连接时发生错误: {}",err);
        process::exit(1);
    }) ;  //创建连接

    return conn;
}



