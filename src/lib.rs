/*
@author: xiao cai niao
@datetime: 2019/9/17
*/

pub mod readvalue;
pub mod meta;
pub mod io;
use std::str;


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
}

#[derive(Debug)]
pub struct Config {
    pub host_info: String,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub program_name: String,
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut host_info = String::from("");
        let mut user_name = String::from("");
        let mut password = String::from("");
        let mut database = String::from("");
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

        Ok(Config { program_name:String::from("rust_test"),host_info, user_name ,password, database})
    }
}

pub fn startop(config: &Config) {
    let conn = io::connection::create_mysql_conn(config) ;  //创建连接

}








