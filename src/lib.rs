/*
@author: xiao cai niao
@datetime: 2019/9/17
*/

pub mod readvalue;
pub mod meta;
pub mod io;
pub mod replication;
use std::str;
use std::process;


use structopt::StructOpt;
use std::fs::OpenOptions;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
pub struct Opt {
    #[structopt(short = "u", long = "user",help = "用户名")]
    pub user: Option<String>,

    #[structopt(short = "p", long = "password",help = "密码")]
    pub password: Option<String>,

    #[structopt(short = "h",long = "host", help="ip地址加端口, ip:port 例如127.0.0.1:3306")]
    pub host: Option<String>,

    #[structopt(short = "D",long = "database", help="库名")]
    pub database: Option<String>,

    #[structopt(short = "c",long = "command", help="sql语句")]
    pub command: Option<String>,

    #[structopt(short = "r",long = "repltype", help="获取binlog模式[repl,file]，也可不设置，用于程序建传递")]
    pub repltype: Option<String>,

    #[structopt(short = "f",long = "file", help="binlog文件，用于文件读取")]
    pub file: Option<String>,

    #[structopt(long = "binlogfile", help="binlog文件名，用于replicaton注册同步")]
    pub binlogfile: Option<String>,

    #[structopt(long = "position", help="注册replication使用的pos")]
    pub position: Option<String>,

    #[structopt(long = "gtid", help="gtid同步模式使用的gtid")]
    pub gtid: Option<String>,

    #[structopt(long = "conntype", help="连接操作类型，repl、command 分别对应slave同步和执行sql")]
    pub conntype: Option<String>,
}

#[derive(Debug)]
pub struct Config {
    pub host_info: String,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub program_name: String,
    pub command: String,
    pub repltype: String,
    pub file: String,
    pub binlogfile: String,
    pub position: String,
    pub gtid: String,
    pub conntype: String,
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut host_info = String::from("");
        let mut user_name = String::from("");
        let mut password = String::from("");
        let mut database = String::from("");
        let mut command = String::from("");
        let mut repltype= String::from("");
        let mut file = String::from("");
        let mut binlogfile = String::from("");
        let mut position = String::from("");
        let mut gtid = String::from("");
        let mut conntype = String::from("");
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

        match args.repltype {
            None => (),
            Some(t) => repltype = t,
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
            Some(t) => position = t,
            _ => {}
        }

        match args.gtid {
            None => (),
            Some(t) => gtid = t,
        }

        match args.conntype {
            None => (),
            Some(t) => conntype = t,
        }

        Ok(Config { program_name:String::from("rust_test"),
            host_info, user_name ,
            password, database,
            command,repltype,file,binlogfile,position,gtid,conntype})
    }
}

pub fn startop(config: &Config) {
    let mut conn = io::connection::create_mysql_conn(config).unwrap_or_else(|err|{
        println!("创建连接时发生错误: {}",err);
        process::exit(1);
    }) ;  //创建连接

    if config.conntype == String::from("command"){
        //let sql = String::from("show master status");
        let values = io::command::execute(&mut conn,&config.command);
        for row in values.iter(){
            println!("{:?}",row);
        }
    }else if config.conntype == String::from("repl") {
        replication::repl_register(&mut conn,&config);
    }else {
        println!("无效的执行参数conntype: {}, --help提供参考",config.conntype);
    }



//    use std::{thread, time};
//    let ten_millis = time::Duration::from_secs(100);
//    thread::sleep(ten_millis);

}








