/*
@author: xiao cai niao
@datetime: 2019/10/11
*/


use std::collections::HashMap;
use crate::replication::readevent::{BinlogEvent,TableMap};
use crate::replication::parsevalue::RowValue;
use crate::replication::readbinlog::Traction;
use crate::replication::parsevalue::MySQLValue;
use failure::_core::str::from_utf8;
use hex;
use bigdecimal::ToPrimitive;
use std::process::id;

//打印sql
pub fn out_sql(data: &Traction, table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,db_tbl: &String, map: &TableMap) {
    match data {
        Traction::GtidEvent(t) => {
            println!("-- GTID: {}:{}", t.gtid,t.gno_id);
        },
        Traction::QueryEvent(t) => {
            println!("use {};",t.database);
            println!("{};", t.command);
        },
        Traction::RowEvent(t,f) => {
            print_command(f, t, table_cols_info, db_tbl, map);
        }
        Traction::XidEvent(_) => {
            println!("COMMIT;");
            println!();

        }
        _ => {}
    }
}

//打印数据
pub fn out_value(data: &Traction, table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,db_tbl: &String){
    match data {
        Traction::GtidEvent(t) => {
            println!("GtidEvent     gtid:{}, gno_id:{}, last_committed:{}, sequence_number:{}",t.gtid,t.gno_id,t.last_committed,t.sequence_number);
        },
        Traction::QueryEvent(t) => {
            println!("QueryEvent    thread_id:{}, database:{}, command:{}",t.thread_id,t.database,t.command);
        },
        Traction::TableMapEvent(t) => {
            println!("TableMap      database_name:{}, table_name:{}",t.database_name,t.table_name);
        },
        Traction::RowEvent(t, f) => {
            print_row_value(f, t,table_cols_info,db_tbl);
        },
        Traction::XidEvent(t) => {
            println!("XidEvent      xid:{}",t.xid);
            //println!("{:?}",a);
            println!();
            println!();
        },
        Traction::RotateLogEvent(t) => {
            println!("{:?}",t);
        }
        Traction::RowEventStatic{ type_code, count } => {
            println!("{:?}    {}bytes",type_code,count);
        }
        Traction::Unknown => {}
        _ => {}
    }
}

//打印统计信息
pub fn out_static() {

}

fn print_row_value(row_values: &RowValue,code: &BinlogEvent, table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,db_tbl: &String) {
    println!("ROW_VALUE");
    match table_cols_info.get(db_tbl) {
        Some(t) => {
            let cols = t;
            let mut del_code = true;
            for row in &row_values.rows {
                match code {
                    BinlogEvent::UpdateEvent => {
                        if del_code {
                            print!("              Before Value ");
                            del_code = false;
                        }else {
                            print!(" After Value ");
                            del_code = true;
                        }
                    }
                    _ => {print!("              ");}
                }
                for (index, value) in row.iter().enumerate(){
                    let col = cols[index].get("COLUMN_NAME").unwrap();
                    match value {
                        Some(MySQLValue::String(t)) => {
                            print!("{}: {}, ", col,t);
                        },
                        Some(MySQLValue::Null) => {
                            print!("{}: {}, ",col, String::from("Null"));
                        }
                        Some(MySQLValue::Json(t)) => {
                            print!("{}: {}, ",col, serde_json::to_string(&t).unwrap());
                        }
                        Some(MySQLValue::Blob(t)) => {
                            let col_type = cols[index].get("COLUMN_TYPE").unwrap();
                            match col_type.find("text") {
                                Some(_) => {
                                    print!("{}: {}, ",col, from_utf8(t).unwrap());
                                    continue;
                                }
                                None => {}
                            }

                            match col_type.find("char") {
                                Some(_) => {
                                    print!("{}: {}, ",col, from_utf8(t).unwrap());
                                    continue;
                                }
                                None => {}
                            }

                            if t.len()> 0{
                                let a = format!("0x{}",hex::encode(t));
                                print!("{}: {},",col, a);
                            }else {
                                print!("{}: {}, ",col, String::from("Null"));
                            }
                        }
                        Some(MySQLValue::SignedInteger(t)) => {
                            print!("{}: {}, ", col, t);
                        }
                        Some(MySQLValue::Decimal(t)) => {
                            print!("{}: {:?}, ", col, t.to_string());
                        }
                        Some(MySQLValue::Date {year, month, day }) => {
                            print!("{}: {}-{}-{}, ", col, year,month,day);
                        }
                        Some(MySQLValue::Year(t)) => {
                            print!("{}: {}, ", col, t);
                        }
                        Some(MySQLValue::Float(t)) => {
                            print!("{}: {}, ", col, t);
                        }
                        Some(MySQLValue::Double(t)) => {
                            print!("{}: {}, ", col,t);
                        }
                        Some(MySQLValue::DateTime { year, month, day, hour, minute, second, subsecond }) => {
                            print!("{}: {}-{}-{} {}:{}:{}.{}, ", col,year,month,day,hour,minute,second,subsecond);
                        }
                        Some(MySQLValue::Enum(t)) => {
                            print!("{}: {}, ", col, t);
                        }
                        Some(MySQLValue::Time { hours, minutes, seconds, subseconds }) => {
                            print!("{}: {}:{}:{}.{}, ", col, hours,minutes,seconds,subseconds);
                        }
                        Some(MySQLValue::Timestamp { unix_time, subsecond }) => {
                            print!("{}: from_unixtime({}.{}), ", col,unix_time,subsecond);
                        }
                        _ => {}
                    }
                }
                if del_code {println!();}
            }
        },
        None => {
            println!("内存中无字段信息: {},{:?}", db_tbl,table_cols_info);
        }
    }

}

fn print_command(
    row_values: &RowValue,code: &BinlogEvent,
    table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,
    db_tbl: &String, map: &TableMap){
    match table_cols_info.get(db_tbl) {
        Some(t) => {
            let cols = t;
            let mut pri_idex = 0 as usize;          //主键索引
            let mut pri = &String::from("");    //主键名称
            let mut pri_info: HashMap<String, usize> = HashMap::new();
            for (idx,r) in cols.iter().enumerate(){
                if r.get("COLUMN_KEY").unwrap() == &String::from("PRI") {
                    pri = r.get("COLUMN_NAME").unwrap();
                    pri_info.insert(pri.parse().unwrap(), idx);
                }
            }

            match code {
                BinlogEvent::UpdateEvent => {
                    println!("-- Update Row Value");
                    let mut a = vec![];
                    let mut rows = vec![];
                    for (i, row) in row_values.rows.iter().enumerate() {
                        match i % 2 {
                            0 => {
                                a.push(row);
                            }
                            1 => {
                                a.push(row);
                                rows.push(a);
                                a = vec![];
                            }
                            _ => {}
                        }
                    }
                    for row in rows{
                        let befor_value = row[0];
                        let after_value = row[1];
                        let v = crate::stdout::outsql::out_update(befor_value, after_value, cols, &pri_info,map);
                        println!("{}",v);
                    }
                }
                BinlogEvent::WriteEvent => {
                    println!("-- Insert Row Value");
                    for row in &row_values.rows {
                        let v = crate::stdout::outsql::out_insert(row, cols, map);
                        println!("{}",v);
                    }
                }
                BinlogEvent::DeleteEvent => {
                    println!("-- Delete Row Value");
                    for row in &row_values.rows {
                        let v = crate::stdout::outsql::out_delete(row, cols, &pri_info, map);
                        println!("{}",v);
                    }
                }
                _ => {}
            }
        }
        None => {
            println!("内存中无字段信息: {},{:?}", db_tbl,table_cols_info);
        }
    }
}