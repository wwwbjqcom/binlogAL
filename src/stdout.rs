/*
@author: xiao cai niao
@datetime: 2019/10/11
*/

use crate::Config;
use std::collections::HashMap;
use crate::replication::readbinlog::Traction;
use crate::replication::readevent::{TableMap};

pub mod outvalue;
pub mod outsql;

//打印输出，打印sql、统计信息、 数据
pub fn format_out(data: &Traction, conf: &Config, table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,db_tbl: &String, map: &TableMap) {
    if conf.statisc {
        //统计每个事务大小, 仅读取binlog文件可用，如果通过replication协议拉过来统计意义不大
        outvalue::out_value(data, table_cols_info,db_tbl);
    } else if conf.getsql {
        //提取sql语句
        outvalue::out_sql(data, table_cols_info,db_tbl, map)
    }
    else {
        //默认直接打印数据
        outvalue::out_value(data, table_cols_info,db_tbl);
    }
}