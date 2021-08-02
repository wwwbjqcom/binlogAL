/*
@author: xiao cai niao
@datetime: 2019/10/21
*/

use crate::Config;
use crate::replication::readbinlog::Traction;
use crate::replication::rollback::RollBackTrac;
use crate::replication::readevent::{EventHeader, GtidEvent, QueryEvent, TableMap};
use crate::replication::readevent;
use std::collections::HashMap;

pub trait UpdateState {
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

#[derive(Debug)]
pub struct GrepGtid{
    pub gtid: String,
    pub state: bool,
    pub start: bool,
}
impl UpdateState for GrepGtid{
    fn start(&mut self){
        self.start = true;
    }
    fn stop(&mut self){
        self.start = false;
    }
}
impl GrepGtid{
    fn new(conf: &Config) -> GrepGtid {
        let mut gtid = String::from("");
        let mut state = false;
        let start = false;
        if conf.gtid.len() > 0 {
            gtid = conf.gtid.parse().unwrap();
            state = true;
        }
        GrepGtid{
            gtid,
            state,
            start
        }
    }
}

#[derive(Debug)]
pub struct GrepDateTime{
    pub start_datetime: usize,
    pub stop_datetime: usize,
    pub state: bool,
    pub start: bool,
}
impl UpdateState for GrepDateTime{}
impl GrepDateTime{
    fn new(conf: &Config) -> GrepDateTime {
        let mut start_datetime: usize = 0;
        let mut stop_datetime: usize = 0;
        let mut state = false;
        if conf.startdatetime.len() > 0 {
            start_datetime = conf.startdatetime.parse().unwrap();
            state = true;
        };
        if conf.stopdatetime.len() > 0 {
            stop_datetime = conf.stopdatetime.parse().unwrap();
        };
        GrepDateTime{
            start_datetime,
            stop_datetime,
            state,
            start: false
        }
    }
}

#[derive(Debug)]
pub struct GrepThreadId{
    thread_id: usize,
    state: bool,
    start: bool,
    pub(crate) gtid_traction: Traction
}
impl UpdateState for GrepThreadId{
    fn start(&mut self){
        self.start = true;
    }
    fn stop(&mut self){
        self.start = false;
    }
}
impl GrepThreadId {
    fn new(conf: &Config) -> GrepThreadId{
        let mut thread_id = 0;
        let mut state = false;
        if conf.threadid.len() > 0 {
            thread_id = conf.threadid.parse().unwrap();
            state = true;
        }
        GrepThreadId{
            thread_id,
            state,
            start: false,
            gtid_traction: Traction::Unknown
        }
    }
}

#[derive(Debug)]
pub struct GrepPosition{
    start_position: usize,
    stop_position: usize,
    state: bool,
    start: bool,
}
impl UpdateState for GrepPosition{}
impl GrepPosition {
    fn new(conf: &Config) -> GrepPosition{
        let mut start_position = 0;
        let mut stop_position = 0;
        let mut state = false;
        if conf.startposition.len() > 0{
            start_position = conf.startposition.parse().unwrap();
            state = true;
        }
        if conf.stopposition.len() > 0 {
            stop_position = conf.stopposition.parse().unwrap();
        }
        GrepPosition{
            start_position,
            stop_position,
            state,
            start: false
        }
    }
}

#[derive(Debug)]
pub struct GrepTbl {
    pub tbl_info: serde_json::Value,
    pub state: bool,                    // 是否有库表过滤项
    pub start: bool,                    // 是否已符合条件
    pub traction_not_db: bool,              // 记录当前事务，当前db是否为需要的, false为不需要的
    pub gtid_traction: Traction,
    pub query_traction: Traction
}
impl UpdateState for GrepTbl{
    fn start(&mut self){
        self.start = true;
    }
    fn stop(&mut self){
        self.start = false;
    }
}
impl GrepTbl {
    fn new(conf: &Config) -> GrepTbl{
        let mut tbl_info = serde_json::from_str("[]").unwrap_or_else(|err|{
            println!("{:?}",err);
            std::process::exit(1);
        });
        let mut state = false;
        if conf.greptbl.len() > 0 {
            tbl_info = serde_json::from_str(&conf.greptbl).unwrap();
            state = true;
        }

        GrepTbl{
            tbl_info,
            state,
            start: false,
            traction_not_db: false,
            gtid_traction: Traction::Unknown,
            query_traction: Traction::Unknown
        }
    }
}

#[derive(Debug)]
pub struct GrepInfo{
    pub grep_gtid: GrepGtid,
    pub grep_date_time: GrepDateTime,
    pub grep_thread_id: GrepThreadId,
    pub grep_position: GrepPosition,
    pub grep_tbl: GrepTbl,
}

impl GrepInfo {
    pub fn new(conf: &Config) -> GrepInfo {
        let grep_gtid = GrepGtid::new(conf);
        let grep_date_time = GrepDateTime::new(conf);
        let grep_thread_id = GrepThreadId::new(conf);
        let grep_position = GrepPosition::new(conf);
        let grep_tbl = GrepTbl::new(conf);
        GrepInfo{
            grep_gtid,
            grep_date_time,
            grep_thread_id,
            grep_position,
            grep_tbl
        }
    }

    fn grep_date_time(&self, rollback_trac: &mut RollBackTrac, event_header: &EventHeader) -> (bool, bool) {
        let grep_datetime_info = &self.grep_date_time;
        // println!("{:?}, {}", &grep_datetime_info, &event_header.timestamp);
        if grep_datetime_info.state {
            if grep_datetime_info.start_datetime > event_header.timestamp as usize {
                rollback_trac.delete_cur_event();
                return (false, false);
            } else {
                if grep_datetime_info.stop_datetime > 0 && grep_datetime_info.stop_datetime < event_header.timestamp as usize {
                    rollback_trac.is_write();
                    return (false, true)
                }
            }
        }
        return (true, false);
    }

    /// 对postion和时间进行过滤
    pub fn grep_pos_time(&self,rollback_trac: &mut RollBackTrac, event_header: &EventHeader) -> (bool,bool) {
        //对postion和datetime进行过滤， 返回元组结果，第一个表示是继续下面的还是continue循环，第二个如果为true表示break
        let grep_position_info = &self.grep_position;
        let grep_datetime_info = &self.grep_date_time;
        return if grep_position_info.state {
            if grep_position_info.stop_position > 0 {
                if grep_position_info.stop_position < event_header.next_position as usize {
                    rollback_trac.is_write();
                    (false, true)
                } else {
                    self.grep_date_time(rollback_trac, event_header)
                }
            } else {
                self.grep_date_time(rollback_trac, event_header)
            }
        } else {
            self.grep_date_time(rollback_trac, event_header)
        }
    }

    /// 进行库表过滤判断
    ///
    /// 如果已经state为true表示已经满足条件，直接返回true
    ///
    /// 如果state为false， 并且为gtid、query、tableevent则返回成功，
    /// 因为这几个都在rowevent前面，需要先执行并保存
    fn grep_tbl_info(&mut self, header: &EventHeader) -> bool {
        let grep_tbl_info = &self.grep_tbl;
        if grep_tbl_info.state {
            return if grep_tbl_info.start {
                if grep_tbl_info.traction_not_db {
                    return match header.type_code {
                        readevent::BinlogEvent::XidEvent => {
                            true
                        }
                        _ => {
                            false
                        }
                    }
                }
                true
            } else {
                match header.type_code {
                    readevent::BinlogEvent::GtidEvent |
                    readevent::BinlogEvent::QueryEvent |
                    readevent::BinlogEvent::TableMapEvent => { true }
                    _ => {
                        false
                    }
                }
            }
        }
        return true;
    }

    /// 在进行event解析前判断是否需要继续，用于对thread_id和 库表过滤项进行判断的前置条件
    pub fn check_repl_grep_status(&mut self, header: &EventHeader) -> bool {
        let grep_threadid_info = &self.grep_thread_id;
        if grep_threadid_info.state {
            if grep_threadid_info.start {
                return self.grep_tbl_info(header);
            }else {
                match header.type_code {
                    readevent::BinlogEvent::GtidEvent |
                    readevent::BinlogEvent::QueryEvent  => {return true;},
                    _ => {return false;}
                }
            }
        } else {
            return self.grep_tbl_info(header);
        }
        return true;
    }

    pub fn save_in_gtid_tmp(&mut self, v: &Traction) -> bool {
        let grep_threadid_info = &self.grep_thread_id;
        if grep_threadid_info.state{
            self.grep_thread_id.gtid_traction = v.clone();
            return false;
        }else if self.grep_tbl.state {
            self.grep_thread_id.gtid_traction = v.clone();
            return false;
        }
        return true;
    }

    pub fn check_grep_gtid(&mut self, v: &GtidEvent) -> bool {
        if self.grep_gtid.state {
            if self.grep_gtid.gtid == format!("{}:{}",v.gtid,v.gno_id){
                self.grep_gtid.start();
                return true;
            }
            return false;
        }
        return true;
    }

    pub fn check_gtid_grep_status(&mut self, header: &EventHeader) -> bool {
        match header.type_code {
            readevent::BinlogEvent::GtidEvent => {},
            _ => {
                if self.grep_gtid.state{
                    if !self.grep_gtid.start{
                        return false;
                    }
                }
            }
        }
        return true;
    }

    pub fn check_grep_threadid(&mut self, v: &QueryEvent, rollback_trac: &mut RollBackTrac) -> bool {
        let grep_thread_id = &self.grep_thread_id;
        if grep_thread_id.state{
            if v.thread_id == grep_thread_id.thread_id as u32 {
                self.grep_thread_id.start();
                if self.grep_tbl.state{
                    self.grep_tbl.query_traction = Traction::QueryEvent(v.clone());
                    rollback_trac.rollback_traction.extend(&rollback_trac.cur_event);
                    return false;
                }
                return true;
            }else {
                rollback_trac.update_event();
                return false;
            }
        }else if self.grep_tbl.state {
            rollback_trac.rollback_traction.extend(&rollback_trac.cur_event);
            self.grep_tbl.query_traction = Traction::QueryEvent(v.clone());
            return false;
        }
        return true;
    }


    /// 库表过滤
    pub fn check_grep_tbl(&mut self, v: &TableMap,
                          rollback_trac: &mut RollBackTrac,
                          conf: &Config,
                          table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,
                          db_tbl: &String) -> bool {
        if self.grep_tbl.state{
            let tbls = &self.grep_tbl.tbl_info[v.database_name.clone()];
            if tbls == &serde_json::Value::String("all".parse().unwrap()){
                // 有过滤项，且是全部提取， 如果不是回滚操作则直接打印前面存储的gtid、query两个event
                self.grep_tbl.start();
                if !conf.rollback{
                    crate::stdout::format_out(&self.grep_thread_id.gtid_traction, conf, table_cols_info, db_tbl, v);
                    crate::stdout::format_out(&self.grep_tbl.query_traction, conf, table_cols_info, db_tbl, v);
                    // 输出后置空， 因为一个事务可能存在多个表
                    // 如果这里不置空，每个tablemap_event满足条件都会打印一次
                    self.reset_traction_value();
                }
            }else {
                // 对库表进行过滤， 这里最多只能设置20个表
                if tbls != &serde_json::Value::Null {
                    'inner: for i in 0..20 {
                        if tbls[i] != serde_json::Value::Null {
                            if tbls[i] == serde_json::Value::String(v.table_name.clone()) {
                                // 如果符合条件则设置self.grep_tbl.start为true
                                self.grep_tbl.start();
                                // 不为rollback则直接打印前面保存的gtid、query两个event
                                if !conf.rollback{
                                    crate::stdout::format_out(&self.grep_thread_id.gtid_traction, conf, table_cols_info, db_tbl, v);
                                    crate::stdout::format_out(&self.grep_tbl.query_traction, conf, table_cols_info, db_tbl, v);
                                    // 输出之后则置空，因为一个事务可能包含多个表
                                    // 当前表不存在过滤表中，则会把sart设置为false
                                    // 如果后面再出现需要的表这里就会多打印一次
                                    self.reset_traction_value();
                                }
                                // break 'inner;
                                self.grep_tbl.traction_not_db = false;
                                return true;
                            }
                        }else {
                            self.grep_tbl.traction_not_db = true;
                            return false;
                        }
                    }
                    self.grep_tbl.traction_not_db = true;
                    return false;
                } else {
                    self.grep_tbl.traction_not_db = true;
                    return false;
                }
            }
        }
        self.grep_tbl.traction_not_db = false;
        true
    }

    fn reset_traction_value(&mut self) {
        self.grep_thread_id.gtid_traction = Traction::Unknown;
        self.grep_tbl.query_traction = Traction::Unknown;
    }
}