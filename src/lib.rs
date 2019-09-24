/*
@author: xiao cai niao
@datetime: 2019/9/17
*/

pub mod readvalue;
pub mod meta;
pub mod io;
use std::str;
use std::process;


use structopt::StructOpt;

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
}

#[derive(Debug)]
pub struct Config {
    pub host_info: String,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub program_name: String,
    pub command: String
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut host_info = String::from("");
        let mut user_name = String::from("");
        let mut password = String::from("");
        let mut database = String::from("");
        let mut command = String::from("");
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
        Ok(Config { program_name:String::from("rust_test"),host_info, user_name ,password, database,command})
    }
}

pub fn startop(config: &Config) {
    let mut conn = io::connection::create_mysql_conn(config).unwrap_or_else(|err|{
        println!("创建连接时发生错误: {}",err);
        process::exit(1);
    }) ;  //创建连接

    //let sql = String::from("show master status");
    let values = io::command::execute(&mut conn,&config.command);
    for row in values.iter(){
        println!("{:?}",row);
    }

//    use std::{thread, time};
//    let ten_millis = time::Duration::from_secs(100);
//    thread::sleep(ten_millis);

}








