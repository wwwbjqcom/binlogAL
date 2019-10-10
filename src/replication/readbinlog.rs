/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use crate::{Config, replication};
use std::net::TcpStream;
use crate::replication::{readevent, parsevalue};
use crate::replication::readevent::{InitValue, EventHeader, InitHeader};
use crate::io::{socketio, pack};
use std::io::Cursor;
use crate::replication::parsevalue::RowValue;

pub enum  ReplType{
    Repl,   //做为slave同步mysql数据
    File,   //从文件读取
    Append  //从远程发送过来的包中解析，用于高可用binlog追加
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
//#[derive(Debug)]
//struct TableRow {
//    table_map: readevent::TableMap,
//    query: readevent::QueryEvent,
//    row_values: Vec<Vec<Option<parsevalue::MySQLValue>>>,
//}
//
//impl TableRow {
//    fn new() -> TableRow {
//        TableRow{
//            table_map: readevent::TableMap {
//                database_name: "".to_string(),
//                table_name: "".to_string(),
//                column_count: 0,
//                column_info: vec![]
//            },
//            query: readevent::QueryEvent {
//                thread_id: 0,
//                execute_seconds: 0,
//                database: "".to_string(),
//                command: "".to_string()
//            },
//            row_values: vec![],
//        }
//    }
//}
//#[derive(Debug)]
//struct Transaction {
//    gtid_log: readevent::GtidEvent,
//    table_row: Vec<TableRow>,
//    xid_event: u8,
//}
//
//impl Transaction{
//    fn new() -> Transaction {
//        Transaction {
//            gtid_log: readevent::GtidEvent {
//                gtid: Default::default(),
//                gno_id: 0,
//                last_committed: 0,
//                sequence_number: 0
//            },
//            table_row: vec![],
//            xid_event: 0
//        }
//    }
//}

//操作binlog数据的入口
pub fn readbinlog(conn: &mut TcpStream, conf: &Config) {
    let mut tabl_map = readevent::TableMap::new();
    loop {
        let (mut buf, header) = socketio::get_packet_from_stream(conn);

        if !pack::check_pack(&buf){
            let err = pack::erro_pack(&buf);
            println!("注册slave发生错误:{}",err);
            return;
        }

        let mut cur = Cursor::new(buf);

        let event_header: EventHeader = readevent::InitHeader::new(&mut cur,conf);
        match event_header.type_code {
            readevent::BinlogEvent::GtidEvent => {
                let a = readevent::GtidEvent::read_event( &event_header, &mut cur);
                println!("GtidEvent     gtid:{}, gno_id:{}, last_committed:{}, sequence_number:{}",a.gtid,a.gno_id,a.last_committed,a.sequence_number);
                //println!("{:?}",a);
            },
            readevent::BinlogEvent::QueryEvent => {
                let a = readevent::QueryEvent::read_event( &event_header, &mut cur);
                println!("QueryEvent    thread_id:{}, database:{}, command:{}",a.thread_id,a.database,a.command);
                //println!("{:?}",a);
            },
            readevent::BinlogEvent::TableMapEvent => {
                let a = readevent::TableMap::read_event( &event_header, &mut cur);
                //println!("{:?}",a);
                println!("TableMap      database_name:{}, table_name:{}",a.database_name,a.table_name);
                tabl_map = a;
            },
            readevent::BinlogEvent::UpdateEvent |
            readevent::BinlogEvent::DeleteEvent |
            readevent::BinlogEvent::WriteEvent => {
                let a = parsevalue::RowValue::read_row_value(&mut cur, &tabl_map, &event_header);
                print_row_value(&a);
            },
            readevent::BinlogEvent::XidEvent => {
                let a = readevent::XidEvent::read_event(&event_header,&mut cur);
                println!("XidEvent      xid:{}",a.xid);
                //println!("{:?}",a);
                println!();
                println!();
            },
            readevent::BinlogEvent::XAPREPARELOGEVENT => {},
            readevent::BinlogEvent::UNKNOWNEVENT => {}
            readevent::BinlogEvent::RotateLogEvent => {
                let a = readevent::RotateLog::read_event( &event_header, &mut cur);
                println!("{:?}",a);
            }
        }
    }
}

fn print_row_value(row_values: &RowValue) {
    println!("ROW_VALUE");
    for row in &row_values.rows {
        println!("              {:?}",row);
    }
}

