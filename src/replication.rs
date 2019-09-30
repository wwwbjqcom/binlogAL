/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use std::net::TcpStream;
use crate::{replication,Config,readvalue};
use crate::io::{response,pack,socketio};
use std::process;
use std::collections::HashMap;

pub mod readbinlog;
pub mod readevent;
pub mod parsevalue;
pub mod jsonb;

pub fn repl_register(conn: &mut TcpStream, conf: &Config) {
    let regist_pack = binlog_dump_pack(conf);
    socketio::write_value(conn, &regist_pack).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

//    use std::{thread, time};
//    let ten_millis = time::Duration::from_secs(100);
//    thread::sleep(ten_millis);
    let (event, _) = socketio::get_packet_from_stream(conn);
    if pack::check_pack(&event){
        replication::readbinlog::readbinlog(conn, conf);
    }
    else {
        let err = pack::erro_pack(&event);
        println!("注册slave发生错误:{}",pack::erro_pack(&event));
    }



}

/*
com_binlog_dump:
    Format for mysql packet position
    dump_type: 1bytes
    position: 4bytes
    flags: 2bytes
        0: BINLOG_DUMP_BLOCK
        1: BINLOG_DUMP_NON_BLOCK
    server_id: 4bytes
    log_file
*/
fn binlog_dump_pack(conf: &Config) -> Vec<u8> {
    let mut pack = vec![];
    let com_binlog_dump = 0x12 as u8;
    let flags = 0;
    pack.push(com_binlog_dump);
    pack.extend(readvalue::write_u32(conf.position.parse().unwrap()));
    pack.extend(readvalue::write_u16(flags));
    pack.extend(readvalue::write_u32(133));
    pack.extend(conf.binlogfile.clone().into_bytes());
    let mut pack_all = response::pack_header(&pack,0);
    pack_all.extend(pack);
    pack_all
}