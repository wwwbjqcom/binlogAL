/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use crate::Config;
use std::net::TcpStream;
use crate::replication::readevent;
use crate::replication::readevent::{InitHeader, InitValue, EventHeader};

pub enum  ReplType{
    Repl,   //做为slave同步mysql数据
    File,   //从文件读取
    append  //从远程发送过来的包中解析，用于高可用binlog追加
}

impl ReplType{
    fn get(conf: &Config) -> ReplType {
        if conf.repltype == String::from("repl") {
            return ReplType::Repl;
        }else if conf.repltype == String::from("file") {
            return ReplType::File;
        }else {
            return ReplType::File;
        }
    }
}

//操作binlog数据的入口
pub fn readbinlog(buf: &Vec<u8>, conf: &Config) {
//    let repl_type = ReplType::get(conf);
//    match repl_type {
//        ReplType::Repl => {
//            let a = readevent::EventHeader::new();
//        } //slave 操作
//        ReplType::File => {} //读取文件
//        ReplType::append => {} //程序见传递
//    }
    let event_header: EventHeader = readevent::InitHeader::new(buf,conf);
    match event_header.type_code {
        readevent::BinlogEvent::GtidEvent => {
            let a = readevent::GtidEvent::read_event( &event_header, &buf);
            println!("{:?}",a);
            return;
        },
        readevent::BinlogEvent::QueryEvent => {
            let a = readevent::QueryEvent::read_event( &event_header, &buf);
            println!("{:?}",a);
            return;
        },
        readevent::BinlogEvent::TableMapEvent => {
            let a = readevent::TableMap::read_event( &event_header, &buf);
            println!("{:?}",a);
            return;
        },
        readevent::BinlogEvent::WriteEvent => {},
        readevent::BinlogEvent::UpdateEvent => {},
        readevent::BinlogEvent::WriteEvent => {},
        readevent::BinlogEvent::DeleteEvent => {},
        readevent::BinlogEvent::XidEvent => {},
        readevent::BinlogEvent::XAPREPARELOGEVENT => {},
        readevent::BinlogEvent::UNKNOWNEVENT => {}
        readevent::BinlogEvent::RotateLogEvent => {
            let a = readevent::RotateLog::read_event( &event_header, &buf);
            println!("{:?}",a);
            return;
        }
        _ => {}
    }


}

