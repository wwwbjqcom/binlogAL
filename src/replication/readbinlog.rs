/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use crate::{Config, replication};
use std::net::TcpStream;
use crate::replication::{readevent, parsevalue, grep};
use crate::replication::readevent::{InitValue, EventHeader, InitHeader, Tell};
use crate::io::{socketio, pack};
use std::io::{Cursor, Read, Write, Seek, SeekFrom};
use crate::replication::parsevalue::RowValue;
use std::collections::HashMap;
use std::io::BufReader;
use std::fs::File;
use serde_json;
use serde_json::Value;
use crate::replication::rollback;
use crate::replication::rollback::{ RollBackTrac};
use crate::replication::grep::UpdateState;

struct GrepInfo{
    grep_gtid: CheckGrepStatus,
    grep_date_time: CheckGrepStatus,
    grep_thread_id: CheckGrepStatus,
    grep_position: CheckGrepStatus,
    grep_tbl: CheckGrepStatus,
}

//用于判断过滤状态，repl模式只能过滤thread_id， 从文件读取全部适用
#[derive(Debug, Clone)]
enum CheckGrepStatus {
    GrepGtid{state: bool, gtid: String},
    GrepDateTime{state: bool, start_time: usize, stop_time: usize},
    GrepThreadId{state: bool, thread_id: usize},
    GrepPosition{state: bool, start_position: usize, stop_position: usize},
    GrepTbl{state: bool},
    Unknown
}

impl CheckGrepStatus{
    fn new_struct(conf: &Config) -> GrepInfo{
        let mut grep_gtid = CheckGrepStatus::Unknown;
        let mut grep_date_time = CheckGrepStatus::Unknown;
        let mut grep_thread_id = CheckGrepStatus::Unknown;
        let mut grep_position = CheckGrepStatus::Unknown;
        let mut grep_tbl = CheckGrepStatus::Unknown;
        if conf.threadid.len() > 0 {
            grep_thread_id = CheckGrepStatus::GrepThreadId {state: false, thread_id: conf.threadid.parse().unwrap()};
        }
        if conf.startdatetime.len() > 0 {
            grep_date_time = CheckGrepStatus::GrepDateTime {state: true, start_time: conf.startdatetime.parse().unwrap(), stop_time: conf.stopdatetime.parse().unwrap() };
        }
        if conf.gtid.len() > 0 {
            grep_gtid = CheckGrepStatus::GrepGtid {state: false, gtid: conf.gtid.parse().unwrap()};
        }
        if conf.greptbl.len() > 0 {
            grep_tbl = CheckGrepStatus::GrepTbl { state: false};
        }
        if conf.startposition.len() > 0 {
            let mut stop_position = 0;
            if conf.stopposition.len() > 0 {
                stop_position = conf.stopposition.parse().unwrap();
            }
            grep_position = CheckGrepStatus::GrepPosition {
                state: true,
                start_position: conf.startposition.parse().unwrap(),
                stop_position
            }
        }
        GrepInfo{
            grep_gtid,
            grep_date_time,
            grep_thread_id,
            grep_position,
            grep_tbl
        }
    }

    fn new(conf: &Config) -> CheckGrepStatus{
        let conf = conf.clone();
        if conf.threadid.len() > 0 {
            return CheckGrepStatus::GrepThreadId {state: false, thread_id: conf.threadid.parse().unwrap()};
        }else if conf.gtid.len() > 0 {
            return CheckGrepStatus::GrepGtid {state: false, gtid: conf.gtid};
        } else {
            return CheckGrepStatus::Unknown;
        }
    }

    fn update(&self) -> Self {
        match self {
            CheckGrepStatus::GrepThreadId { state, thread_id } => {
                CheckGrepStatus::GrepThreadId {state: true, thread_id: *thread_id }
            }
            CheckGrepStatus::GrepGtid { state, gtid } => {
                CheckGrepStatus::GrepGtid {state: true, gtid: gtid.parse().unwrap() }
            }
            CheckGrepStatus::GrepTbl{ state } => {
                CheckGrepStatus::GrepTbl {state: true}
            }
            _ => {
                CheckGrepStatus::Unknown
            }
        }
    }

    fn init(&self) -> Self {
        match self {
            CheckGrepStatus::GrepThreadId { state, thread_id } => {
                CheckGrepStatus::GrepThreadId {state: false, thread_id: *thread_id }
            }
            CheckGrepStatus::GrepGtid { state, gtid } => {
                CheckGrepStatus::GrepGtid {state: false, gtid: gtid.parse().unwrap() }
            }
            CheckGrepStatus::GrepDateTime { state, start_time, stop_time } => {
                CheckGrepStatus::GrepDateTime {state: false, start_time: *start_time, stop_time: *stop_time }
            }
            CheckGrepStatus::GrepTbl{ state }=> {
                CheckGrepStatus::GrepTbl {state: false}
            }
            _ => {
                CheckGrepStatus::Unknown
            }
        }
    }
}
#[derive(Debug, Clone)]
pub enum Traction{
    GtidEvent(readevent::GtidEvent),
    QueryEvent(readevent::QueryEvent),
    TableMapEvent(readevent::TableMap),
    RowEvent(readevent::BinlogEvent,parsevalue::RowValue),
    XidEvent(readevent::XidEvent),
    RotateLogEvent(readevent::RotateLog),
    RowEventStatic{type_code: readevent::BinlogEvent,count: usize},
    Unknown,
}


//从文件读取binlog
pub fn readbinlog_fromfile(conf: &Config, version: &u8, reader: &mut BufReader<File>) {
    //首先获取文件大小
    reader.seek(SeekFrom::End(0));
    let reader_size = reader.tell().unwrap();
    //

    let mut tabl_map = readevent::TableMap::new();
    let mut table_cols_info: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    let mut db_tbl = String::from("");

    //回滚变量设置
    let mut rollback_trac = RollBackTrac::new(reader, conf);

    //设置过滤状态部分
    let mut grep_info = grep::GrepInfo::new(conf);
    let mut check_status = false;

    //

    'all: loop {
        rollback_trac.cur_event= vec![];
        let mut header_buf = vec![0u8; 19];
        let cur_tell = reader.tell().unwrap();
        if conf.rollback{
            if cur_tell + 19 > reader_size {
                rollback_trac.write_rollback_log();
            }
        }

        reader.read_exact(header_buf.as_mut()).unwrap_or_else(|err|{
            println!("{}",err);
            std::process::exit(1);
        });
        rollback_trac.append_cur_event(&header_buf);

        let mut cur = Cursor::new(header_buf);
        let event_header: EventHeader = readevent::InitHeader::new(&mut cur,conf);
        let payload = event_header.event_length as usize - event_header.header_length as usize;
        let mut payload_buf = vec![0u8; payload];
        reader.read_exact(payload_buf.as_mut());
        rollback_trac.append_cur_event(&payload_buf);
        let mut cur = Cursor::new(payload_buf);
        //判断gtid提取情况
        if !grep_info.check_gtid_grep_status(&event_header) {
            continue 'all;
        }

        //判断position和datetime过滤情况
        let (grep_state,success )= grep_info.grep_pos_time(&mut rollback_trac, &event_header);
        if success{
            break 'all;
        }else if !grep_state {
            continue 'all;
        }
        //
        check_status = grep_info.check_repl_grep_status(&event_header);
        if !check_status {
            rollback_trac.delete_cur_event();
            continue;
        };
        let mut data = Traction::Unknown;
        match event_header.type_code {
            readevent::BinlogEvent::GtidEvent => {
                rollback_trac.init_traction_buf();
                let v = readevent::GtidEvent::read_event( &event_header, &mut cur, version);

                if !grep_info.check_grep_gtid(&v){
                    rollback_trac.delete_cur_event();
                    continue 'all;
                }

                data = Traction::GtidEvent(v);
                if !grep_info.save_in_gtid_tmp(&data){
                    rollback_trac.rollback_traction.extend(&rollback_trac.cur_event);
                    continue 'all;
                }
            },
            readevent::BinlogEvent::QueryEvent => {
                let v = readevent::QueryEvent::read_event( &event_header, &mut cur, version);
                if !grep_info.check_grep_threadid(&v, &mut rollback_trac){
                    continue 'all;
                }
                if !conf.rollback{
                    crate::stdout::format_out(&grep_info.grep_thread_id.gtid_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                    data = Traction::QueryEvent(v);
                }
            },
            readevent::BinlogEvent::TableMapEvent => {
                let v = readevent::TableMap::read_event( &event_header, &mut cur, version);
                if !grep_info.check_grep_tbl(&v, &mut rollback_trac, conf, &mut table_cols_info, &db_tbl){
                    continue 'all;
                }
                db_tbl = format!("{}.{}", v.database_name, v.table_name).clone();
                crate::meta::get_col(conf, &v.database_name, &v.table_name, &mut table_cols_info);
                tabl_map = v.clone();
                data = Traction::TableMapEvent(v);
            },
            readevent::BinlogEvent::UpdateEvent |
            readevent::BinlogEvent::DeleteEvent |
            readevent::BinlogEvent::WriteEvent => {
                if conf.rollback{
                    rollback_trac.rollback_traction.extend(rollback::rollback_row_event(&rollback_trac.cur_event, &event_header, &tabl_map));

                } else if conf.statisc{
                    data = Traction::RowEventStatic{type_code: event_header.type_code.clone(),count:event_header.event_length as usize};
                }else {
                    let read_type = crate::meta::ReadType::File;
                    let v = parsevalue::RowValue::read_row_value(&mut cur, &tabl_map, &event_header,&read_type);
                    data = Traction::RowEvent(event_header.type_code.clone(),v);
                }
            },
            readevent::BinlogEvent::XidEvent => {
                if !conf.rollback{
                    data = Traction::XidEvent(readevent::XidEvent::read_event(&event_header,&mut cur, version));

                    if grep_info.grep_gtid.start{
                        crate::stdout::format_out(&data, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                        break 'all;
                    }
                }

                if check_status {
                    //重新初始化状态
                    grep_info.grep_tbl.stop();
                    grep_info.grep_thread_id.stop();
                }
            },
            readevent::BinlogEvent::XAPREPARELOGEVENT => {},
            readevent::BinlogEvent::UNKNOWNEVENT => {
                rollback_trac.delete_cur_event();
                continue 'all;
            }
            readevent::BinlogEvent::RotateLogEvent => {
                data = Traction::RotateLogEvent(readevent::RotateLog::read_event(&event_header, &mut cur, version));
            }
            _ => {}
        }

        if !conf.rollback{
            crate::stdout::format_out(&data, conf, &mut table_cols_info, &db_tbl, &tabl_map);
        }else {
            match event_header.type_code {
                readevent::BinlogEvent::XidEvent => {
                    rollback_trac.rollback_traction.extend(rollback_trac.cur_event.clone());
                    let tra_len = rollback_trac.rollback_traction.len();
                    rollback_trac.events.push(rollback_trac.rollback_traction.clone());
                    rollback_trac.count += tra_len;
                    if rollback_trac.check_file_size(){
                        rollback_trac.update();
                    }

                    if grep_info.grep_gtid.start{
                        rollback_trac.write_rollback_log();
                        break 'all;
                    }
                }
                readevent::BinlogEvent::WriteEvent|
                readevent::BinlogEvent::UpdateEvent|
                readevent::BinlogEvent::DeleteEvent=> {

                }
                _ => {
                    rollback_trac.rollback_traction.extend(rollback_trac.cur_event.clone());
                }
            }
            rollback_trac.cur_event= vec![];
        }
    }
}

//操作binlog数据的入口
pub fn readbinlog(conn: &mut TcpStream, conf: &Config, version: &u8) {
    let mut tabl_map = readevent::TableMap::new();
    let mut table_cols_info: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    let mut db_tbl = String::from("");

    //设置过滤状态部分
    let grep_status_info = CheckGrepStatus::new_struct(conf);
    let mut grep_threadid_info = grep_status_info.grep_thread_id;
    let mut grep_tbl_info = grep_status_info.grep_tbl;
    let mut tbl_info: Value = serde_json::from_str("{}").unwrap();
    let (mut grep_threadid, mut grep_tbl) = (false, false);
    match grep_threadid_info {
        CheckGrepStatus::GrepThreadId { state, thread_id } => {
            grep_threadid = true;
        }
        _ => {}
    }
    match grep_tbl_info {
        CheckGrepStatus::GrepTbl { state } => {
            grep_tbl = true;
            tbl_info = serde_json::from_str(&conf.greptbl).unwrap();
        }
        _ => {}
    }
    //

    //let mut grep_status = CheckGrepStatus::new(conf);
    let mut gtid_traction = Traction::Unknown;
    let mut query_traction = Traction::Unknown;
    let mut check_status = false;
    'all: loop {
        let (buf, _) = socketio::get_packet_from_stream(conn);

        if !pack::check_pack(&buf){
            let err = pack::erro_pack(&buf);
            println!("注册slave发生错误:{}",err);
            return;
        }
        let mut cur = Cursor::new(buf);

        let event_header: EventHeader = readevent::InitHeader::new(&mut cur,conf);
        check_status = check_repl_grep_status(&grep_threadid_info, &grep_tbl_info, &event_header);
        if !check_status {
            continue;
        }
        let mut data = Traction::Unknown;
        match event_header.type_code {
            readevent::BinlogEvent::GtidEvent => {
                if grep_threadid {
                    match grep_threadid_info {
                        CheckGrepStatus::GrepThreadId { state, thread_id} => {
                            //thread_id只存在于query_event， gtid_event在其之前，所以需要临时存储
                            gtid_traction = Traction::GtidEvent(readevent::GtidEvent::read_event( &event_header, &mut cur, version));
                        }
                        _ => {continue;}
                    }
                }
                else if grep_tbl {
                    gtid_traction = Traction::GtidEvent(readevent::GtidEvent::read_event( &event_header, &mut cur, version));
                }
                else {
                    data = Traction::GtidEvent(readevent::GtidEvent::read_event( &event_header, &mut cur, version));
                }
            },
            readevent::BinlogEvent::QueryEvent => {
                let v = readevent::QueryEvent::read_event( &event_header, &mut cur, version);
                if grep_threadid{
                    match grep_threadid_info {
                        CheckGrepStatus::GrepThreadId { state, thread_id } => {
                            //如果过滤thread_id在此进行判断
                            if v.thread_id == thread_id as u32 {
                                grep_threadid_info = grep_threadid_info.update();
                                if  grep_tbl {
                                    query_traction = Traction::QueryEvent(v);
                                }
                                else {
                                    crate::stdout::format_out(&gtid_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                                    data = Traction::QueryEvent(v);
                                }
                            }
                            else { continue; }
                        }
                        _ => {continue;}
                    }
                }
                else if grep_tbl {
                    query_traction = Traction::QueryEvent(v);
                }
                else {
                    data = Traction::QueryEvent(v);
                }


            },
            readevent::BinlogEvent::TableMapEvent => {
                let a = readevent::TableMap::read_event( &event_header, &mut cur, version);
                match grep_tbl_info {
                    CheckGrepStatus::GrepTbl { state } => {
                        let tbls = &tbl_info[a.database_name.clone()];
                        if tbls == &serde_json::Value::String("all".parse().unwrap()){
                            grep_tbl_info = grep_tbl_info.update();
                            crate::stdout::format_out(&gtid_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                            crate::stdout::format_out(&query_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                        }
                        else {
                            if tbls != &serde_json::Value::Null {
                                'inner: for i in 0..20 {
                                    if tbls[i] != serde_json::Value::Null {
                                        if tbls[i] == serde_json::Value::String(a.table_name.clone()) {
                                            grep_tbl_info = grep_tbl_info.update();
                                            crate::stdout::format_out(&gtid_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                                            crate::stdout::format_out(&query_traction, conf, &mut table_cols_info, &db_tbl, &tabl_map);
                                            break 'inner;
                                        }
                                    }else {
                                        continue 'all;
                                    }
                                }
                            }
                            else { continue 'all; }
                        }
                    }
                    _ => {}
                }
                db_tbl = format!("{}.{}", a.database_name, a.table_name).clone();
                crate::meta::get_col(conf, &a.database_name, &a.table_name, &mut table_cols_info);
                tabl_map = a.clone();
                data = Traction::TableMapEvent(a);
            },
            readevent::BinlogEvent::UpdateEvent |
            readevent::BinlogEvent::DeleteEvent |
            readevent::BinlogEvent::WriteEvent => {
                let read_type = crate::meta::ReadType::Repl;
                let v = parsevalue::RowValue::read_row_value(&mut cur, &tabl_map, &event_header,&read_type);
                data = Traction::RowEvent(event_header.type_code.clone(),v);
            },
            readevent::BinlogEvent::XidEvent => {
                data = Traction::XidEvent(readevent::XidEvent::read_event(&event_header,&mut cur, version));
                if check_status {
                    //重新初始化状态
                    grep_threadid_info = grep_threadid_info.init();
                    grep_tbl_info = grep_tbl_info.init();
                }
            },
            readevent::BinlogEvent::XAPREPARELOGEVENT => {},
            readevent::BinlogEvent::UNKNOWNEVENT => {}
            readevent::BinlogEvent::RotateLogEvent => {
                data = Traction::RotateLogEvent(readevent::RotateLog::read_event(&event_header, &mut cur, version));
            }
            _ => {}
        }

        crate::stdout::format_out(&data, conf, &mut table_cols_info, &db_tbl, &tabl_map);
    }
}

fn check_repl_grep_status(grep_status: &CheckGrepStatus, grep_tbl_info: &CheckGrepStatus, header: &EventHeader) -> bool {
    match grep_status {
        CheckGrepStatus::GrepThreadId { state, thread_id } => {
            if *state {
                match grep_tbl_info {
                    CheckGrepStatus::GrepTbl { state } => {
                        if *state {
                            return true;
                        }
                        else {
                            match header.type_code {
                                readevent::BinlogEvent::GtidEvent |
                                readevent::BinlogEvent::QueryEvent |
                                readevent::BinlogEvent::TableMapEvent => {return true;}
                                _ => {return false;}
                            };
                        }
                    }
                    _ => {}
                }
                return true;
            }else {
                match header.type_code {
                    readevent::BinlogEvent::GtidEvent |
                    readevent::BinlogEvent::QueryEvent => {return true;},
                    _ => {return false;}
                }
            }
        }
        _ => {
            match grep_tbl_info {
                CheckGrepStatus::GrepTbl { state } => {
                    if *state {
                        return true;
                    }
                    else {
                        match header.type_code {
                            readevent::BinlogEvent::GtidEvent |
                            readevent::BinlogEvent::QueryEvent |
                            readevent::BinlogEvent::TableMapEvent => {return true;}
                            _ => {return false;}
                        };
                    }
                }
                _ => {}
            }
            return true;
        }
    }
}



