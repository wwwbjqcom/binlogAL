/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use std::net::TcpStream;
use crate::{replication, Config, readvalue, io};
use crate::io::{response,socketio};
use std::process;
use serde_json::from_str;
use std::io::{BufReader, Seek, SeekFrom};
use std::fs::File;

pub mod readbinlog;
pub mod readevent;
pub mod parsevalue;
pub mod jsonb;
pub mod rollback;
pub mod grep;

pub fn repl_register(conn: &mut TcpStream, conf: &Config) {
    let version = get_version(conn);
    if conf.runtype == String::from("repl"){
        check_sum(conn);
        let mut regist_pack= vec![];
        if conf.gtid.len() > 0 {
            regist_pack = gtid_dump_pack(conf);
        }else if conf.binlogfile.len() > 0 {
            regist_pack = binlog_dump_pack(conf);
        } else {
            println!("主从同步配置项错误，gtid/binlog模式必须给定其一的参数");
            process::exit(1);
        }
        socketio::write_value(conn, &regist_pack).unwrap_or_else(|err|{
            println!("{}",err);
            process::exit(1);
        });
        replication::readbinlog::readbinlog(conn, conf,&version);
    }else if conf.runtype == String::from("file") {
        let f = File::open(&conf.file).unwrap_or_else(|err|{
            println!("创建文件({})访问发生错误:{}",conf.file, err);
            process::exit(1);
        });
        let mut reader = BufReader::new(f);
//        if conf.startposition.len() > 0 {
//            reader.seek(SeekFrom::Current(conf.startposition.parse().unwrap()));
//        }else { reader.seek(SeekFrom::Current(4)); }
        replication::readbinlog::readbinlog_fromfile(conf, &version, &mut reader)
    }

}

fn check_sum(conn: &mut TcpStream) {
    let sql = String::from("select @@BINLOG_CHECKSUM as checksum;");
    let values = io::command::execute(conn,&sql);
    for row in values.iter(){
        for (_, value) in row{
            if value.len() > 0 {
                let sql = String::from("set @master_binlog_checksum= @@global.binlog_checksum;");
                io::command::execute_update(conn,&sql);
                return;
            }
        }
    }
}

fn get_version(conn: &mut TcpStream) -> u8 {
    let sql = String::from("select @@version;");
    let mut v = 0 as u8;
    let values = io::command::execute(conn,&sql);
    for row in values.iter(){
        for (_, value) in row{
            let a: Vec<&str> = value.split(".").collect();
            v = from_str::<u8>(a[0]).unwrap();
        }
    }
    v
}

/*
com_binlog_dump:
    Format for mysql packet position
    Packet type: 1bytes
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
    pack.extend(readvalue::write_u32(conf.serverid.parse().unwrap()));
    pack.extend(conf.binlogfile.clone().into_bytes());
    let mut pack_all = response::pack_header(&pack,0);
    pack_all.extend(pack);
    pack_all
}
/*
    Format for mysql packet master_auto_position

    All fields are little endian
    All fields are unsigned

    Packet type     byte   1byte   == 0x1e
    Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
    Server id       uint   4bytes
    binlognamesize  uint   4bytes
    binlogname      str    Nbytes  N = binlognamesize
                                   Zeroified
    binlog position uint   4bytes  == 4
    payload_size    uint   4bytes

    What come next, is the payload, where the slave gtid_executed
    is sent to the master
    n_sid           ulong  8bytes  == which size is the gtid_set
    | sid           uuid   16bytes UUID as a binary
    | n_intervals   ulong  8bytes  == how many intervals are sent for this gtid
    | | start       ulong  8bytes  Start position of this interval
    | | stop        ulong  8bytes  Stop position of this interval

    A gtid set looks like:
      19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
      1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140

    In this particular gtid set, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
    is the first member of the set, it is called a gtid.
    In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
    and have two intervals, 1-3 and 8-10, 1 is the start position of the first interval
    3 is the stop position of the first interval.
*/

fn gtid_dump_pack(conf: &Config) -> Vec<u8> {
    let mut pack = vec![];
    let com_binlog_dump_gtid = 0x1e as u8;
    let flags = 0;  //BINLOG_DUMP_BLOCK
    pack.push(com_binlog_dump_gtid);
    pack.extend(readvalue::write_u16(flags));
    pack.extend(readvalue::write_u32(conf.serverid.parse().unwrap()));
    pack.extend(readvalue::write_u32(3));   //binlognamesize
    pack.extend(&[0u8;3]);   //binlogname
    pack.extend(readvalue::write_u64(4));   //binlog_pos_info
    let gtids = gtid_parse(&conf.gtid);
    let (gtid_prue, encode_length) = gtid_encode(&gtids);

    pack.extend(readvalue::write_u32(encode_length as u32));
    pack.extend(gtid_prue);
    let mut pack_all = response::pack_header(&pack,0);
    pack_all.extend(pack);
    pack_all
}

fn gtid_encode(gtids: &Vec<GtidSet>) -> (Vec<u8>, usize){
    let mut gtid_prue = vec![];
    let mut encode_length: usize = 8;  //n_sid 字节长度
    let n_sid = gtids.len();
    gtid_prue.extend(readvalue::write_u64(n_sid as u64));
    for gtidset in gtids{
        let sid = &gtidset.sid;
        let a = uuid::Uuid::from(sid.parse().unwrap());
        let interval_len = gtidset.intervals.len();
        encode_length += 16 + 8 + (interval_len * (8 + 8)); //一个sid 加 多个pos起始、终止位置的长度
        gtid_prue.extend(a.as_bytes());
        gtid_prue.extend(readvalue::write_u64(gtidset.intervals.len() as u64));
        for inter in &gtidset.intervals{
            gtid_prue.extend(readvalue::write_u64(inter.interval_start as u64));
            gtid_prue.extend(readvalue::write_u64(inter.interval_stop as u64));
        }
    }
    (gtid_prue, encode_length)
}

struct GtidIntervals{
    interval_start: usize,
    interval_stop: usize
}
impl GtidIntervals{
    fn new(interval: &str) -> GtidIntervals{
        let mut interval_start: usize = 0;
        let mut interval_stop: usize = 0;
        let intervals: Vec<&str> = interval.split("-").collect();
        match intervals.len() {
            1 =>{
                interval_start = intervals[0].parse::<usize>().unwrap();
                interval_stop = interval_start + 1;
            }
            2 => {
                interval_start = intervals[0].parse::<usize>().unwrap();
                interval_stop = intervals[1].parse::<usize>().unwrap() + 1;
            }
            _ => {}
        }

        GtidIntervals{
            interval_start,
            interval_stop
        }
    }
}

struct GtidSet {
    sid: String,
    intervals: Vec<GtidIntervals>
}
impl GtidSet{
    fn new(gtid: &str) -> GtidSet {
        let mut intervals: Vec<GtidIntervals>= vec![];
        let gtid_info: Vec<&str> = gtid.split(":").collect();
        let sid = gtid_info[0].parse().unwrap();
        for (idx,interval) in gtid_info.iter().enumerate(){
            if idx > 0{
                intervals.push(GtidIntervals::new(interval));
            }
        }

        GtidSet{
            sid ,
            intervals
        }
    }
}

fn gtid_parse(gtid: &String) -> Vec<GtidSet> {
    let mut gtids = vec![];
    let conf_gtids: Vec<&str> = gtid.split(",").collect();
    for gtid_info in conf_gtids{
        gtids.push(GtidSet::new(gtid_info));
    }
    gtids
}