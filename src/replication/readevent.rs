/*
@author: xiao cai niao
@datetime: 2019/9/25
*/
use crate::{readvalue, Config};
use crate::meta;
use std::process;
use crate::readvalue::read_string_value;
use std::borrow::Borrow;
use uuid;
use uuid::Uuid;
use std::io::Read;


#[derive(Debug)]
pub enum BinlogEvent{
    QueryEvent,
    RotateLogEvent,
    TableMapEvent,
    GtidEvent,
    UpdateEvent,
    WriteEvent,
    DeleteEvent,
    XidEvent,
    XAPREPARELOGEVENT,
    UNKNOWNEVENT
}

pub trait InitHeader{
    fn new(buf: &Vec<u8>, conf: &Config) -> Self;
}

pub trait InitValue{
    fn read_event(header: &EventHeader, buf: &Vec<u8>) -> Self;
}


/*
binlog包头部分
    binlog_event_header_len = 19
    timestamp : 4bytes
    type_code : 1bytes
    server_id : 4bytes
    event_length : 4bytes
    next_position : 4bytes
    flags : 2bytes
*/
#[derive(Debug)]
pub struct EventHeader{
    //19bytes 包头部分
    pub timestamp: u32,
    pub type_code: BinlogEvent,
    pub server_id: u32,
    pub event_length: u32,
    pub next_position: u32,
    pub flags: u16,
    pub header_length: u8,
}

impl InitHeader for EventHeader {
    fn new(buf: &Vec<u8>, conf: &Config) -> EventHeader{
        let mut header_length: u8 = 19;
        let mut offset = 0;
        if conf.conntype == String::from("repl"){
            //如果是模拟slave同步会多亿字节的头部分
            offset += 1;
            header_length += 1;
        }

        let (timestamp,offset) = (readvalue::read_u32(&buf[offset..offset+4]),offset + 4);
        let (type_code,offset) = (Self::get_type_code_event(&Some(buf[offset])),offset+1);
        let (server_id,offset) = (readvalue::read_u32(&buf[offset..offset+4]),offset + 4);
        let (event_length,offset) = (readvalue::read_u32(&buf[offset..offset+4]),offset + 4);
        let (next_position,offset) = (readvalue::read_u32(&buf[offset..offset+4]),offset + 4);
        let flags = readvalue::read_u16(&buf[offset..offset+2]);
        EventHeader{
            timestamp,
            type_code,
            server_id,
            event_length,
            next_position,
            flags,
            header_length
        }
    }
}

impl EventHeader{
    fn get_type_code_event(type_code: &Option<u8>) -> BinlogEvent{
        match type_code {
            Some(4) => BinlogEvent::RotateLogEvent,
            Some(2) => BinlogEvent::QueryEvent,
            Some(33) => BinlogEvent::GtidEvent,
            Some(19) => BinlogEvent::TableMapEvent,
            Some(30) => BinlogEvent::WriteEvent,
            Some(31) => BinlogEvent::UpdateEvent,
            Some(32) => BinlogEvent::DeleteEvent,
            Some(33) => BinlogEvent::GtidEvent,
            Some(16) => BinlogEvent::XidEvent,
            Some(38) => BinlogEvent::XAPREPARELOGEVENT,
            _ => BinlogEvent::UNKNOWNEVENT
        }
    }
}

/*
query_event:
    fix_part = 13:
        thread_id : 4bytes
        execute_seconds : 4bytes
        database_length : 1bytes
        error_code : 2bytes
        variable_block_length : 2bytes
    variable_part :
        variable_block_length = fix_part.variable_block_length
        database_name = fix_part.database_length
        sql_statement = event_header.event_length - 19 - 13 - variable_block_length - database_length - 4
*/
#[derive(Debug)]
pub struct QueryEvent{
    pub thread_id: u32,
    pub execute_seconds: u32,
    pub database: String,
    pub command: String
}

impl InitValue for QueryEvent{
    fn read_event(header: &EventHeader, buf: &Vec<u8>) -> QueryEvent{
        let mut offset = (header.header_length) as usize;
        let (thread_id, offset) = (readvalue::read_u32(&buf[..offset+4]), offset + 4);
        let (execute_seconds, offset) = (readvalue::read_u32(&buf[offset..offset+4]), offset + 4);
        let (database_length, offset) = (buf[offset] as usize, offset + 1);
        let (error_code, offset) = (readvalue::read_u16(&buf[offset..offset+2]), offset + 2);
        let (variable_block_length, mut offset) = (readvalue::read_u16(&buf[offset..offset+2]) as usize, offset + 2);
        offset += variable_block_length;
        let (database, mut offset) = (readvalue::read_string_value(&buf[offset..offset+ database_length]), offset + database_length);
        offset += 1 ;
        let command = readvalue::read_string_value(&buf[offset..]);
        QueryEvent{
            thread_id,
            execute_seconds,
            database,
            command
        }

    }
}


/*
rotate_log_event:
    Fixed data part: 8bytes
    Variable data part: event_length - header_length - fixed_length (string<EOF>)
*/
#[derive(Debug)]
pub struct RotateLog{
    pub binlog_file: String
}

impl InitValue for RotateLog{
    fn read_event(header: &EventHeader, buf: &Vec<u8>) -> RotateLog{
        let fixed_length: usize = 8;
        let offset = fixed_length + header.header_length as usize;
        let binlog_file = readvalue::read_string_value(&buf[offset..]);
        RotateLog{
            binlog_file
        }
    }
}

/*
table_map_event:
    fix_part = 8
        table_id : 6bytes
        Reserved : 2bytes
    variable_part:
        database_name_length : 1bytes
        database_name : database_name_length bytes + 1
        table_name_length : 1bytes
        table_name : table_name_length bytes + 1
        cloums_count : 1bytes
        colums_type_array : one byte per column
        mmetadata_lenth : 1bytes
        metadata : .....(only available in the variable length field，varchar:2bytes，text、blob:1bytes,time、timestamp、datetime: 1bytes
                        blob、float、decimal : 1bytes, char、enum、binary、set: 2bytes(column type id :1bytes metadatea: 1bytes))
        bit_filed : 1bytes
        crc : 4bytes
        .........
*/
#[derive(Debug)]
pub struct TableMap{
    pub database_name: String,
    pub table_name: String,
    pub column_count: u8,
    pub column_type_list: Vec<u8>,
    pub column_meta_list: Vec<Vec<Vec<usize>>>,
}
impl TableMap{
    fn read_column_meta(buf: &Vec<u8>,col_type: &u8,offset: usize) -> (Vec<Vec<usize>>,usize) {
        let mut value: Vec<Vec<usize>> = vec![];
        let mut offset = offset;
        let column_type_info = meta::ColumnTypeDict::new();
        if col_type == &column_type_info.MYSQL_TYPE_VAR_STRING{
            value.push(Self::read_string_meta(&buf[offset..offset + 2]));
            offset += 2;
        }
        else if col_type == &column_type_info.MYSQL_TYPE_VARCHAR {
            value.push(Self::read_string_meta(&buf[offset..offset + 2]));
            offset += 2;
        }
        else if col_type == &column_type_info.MYSQL_TYPE_BLOB {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_MEDIUM_BLOB {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_LONG_BLOB {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_TINY_BLOB {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_JSON {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_TIMESTAMP2 {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_DATETIME2 {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_TIME2 {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_NEWDECIMAL {
            value.push(Self::read_newdecimal(&buf[offset..offset + 2]).to_owned().to_vec());
            offset += 2;
        }else if col_type == &column_type_info.MYSQL_TYPE_FLOAT {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_DOUBLE {
            value.push(vec![buf[offset] as usize]);
            offset += 1;
        }else if col_type == &column_type_info.MYSQL_TYPE_STRING {
            value.push(Self::read_string_type(&buf[offset..offset + 2], col_type).to_owned().to_vec());
            offset += 2;
        }else {
            value.push(vec![0]);
        }
        return (value,offset);
    }

    fn read_string_meta(buf: &[u8]) -> Vec<usize> {
        let metadata = readvalue::read_u16(buf);
        let mut v = vec![];
        if metadata > 255 {
            v.push(2);
        }else {
            v.push(1);
        }
        v
    }

    fn read_newdecimal(buf: &[u8]) -> [usize;2] {
        let precision = buf[0] as usize;
        let decimals = buf[1] as usize;
        [precision,decimals]
    }

    fn read_string_type(buf: &[u8],col_type: &u8) -> [usize;1] {
        let _type = buf[0];
        let metadata = buf[1] as usize;
        if col_type != &_type{
            [65535]
        }else {
            [metadata]
        }
    }

}

impl InitValue for TableMap{
    fn read_event( header: &EventHeader,buf: &Vec<u8>) -> TableMap{
        let table_map_event_fix_length = 8;
        let mut offset = table_map_event_fix_length + header.header_length as usize;
        let (database_length, offset) = (buf[offset] as usize, offset + 1);
        let (database_name, mut offset) = (readvalue::read_string_value(&buf[offset..offset+database_length]), offset + database_length);
        offset += 1;
        let (table_length, offset) = (buf[offset] as usize, offset + 1);
        let (table_name, mut offset) = (readvalue::read_string_value(&buf[offset..offset+table_length]), offset + table_length);
        offset += 1;
        let (column_count, mut offset) = (buf[offset],offset + 1);
        let mut column_type_list = vec![];
        let mut num = 0;
        while num < column_count {
            let column_type= buf[offset];
            offset += 1;
            column_type_list.push(column_type);
            num += 1;
        }
        let mut column_meta_list = vec![];
        for col_type in column_type_list.iter(){
            //let col_meta = vec![];
            let (col_meta, offset) = Self::read_column_meta(buf, col_type,offset);
            column_meta_list.push(col_meta);
        }

        TableMap{
            database_name,
            table_name,
            column_count,
            column_meta_list,
            column_type_list
        }
    }
}

/*
gtid_event:
    The layout of the buffer is as follows:
    +------+--------+-------+-------+--------------+---------------+
    |flags |SID     |GNO    |lt_type|last_committed|sequence_number|
    |1 byte|16 bytes|8 bytes|1 byte |8 bytes       |8 bytes        |
    +------+--------+-------+-------+--------------+---------------+

    The 'flags' field contains gtid flags.
        0 : rbr_only ,"/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*/%s\n"
        1 : sbr

    lt_type (for logical timestamp typecode) is always equal to the
    constant LOGICAL_TIMESTAMP_TYPECODE.

    5.6 did not have TS_TYPE and the following fields. 5.7.4 and
    earlier had a different value for TS_TYPE and a shorter length for
    the following fields. Both these cases are accepted and ignored.

    The buffer is advanced in Binary_log_event constructor to point to
    beginning of post-header
*/

#[derive(Debug)]
pub struct GtidEvent{
    pub gtid: Uuid,
    pub gno_id: u64,
    pub last_committed: u64,
    pub sequence_number: u64
}

impl InitValue for GtidEvent {
    fn read_event(header: &EventHeader, buf: &Vec<u8>) -> GtidEvent {
        let offset = (header.header_length + 1) as usize;
        let (uuid, offset) = (buf[offset..offset+16].to_vec(), offset + 16);
        let mut sid = [0u8;16];
        for i in (0..16){
            sid[i] = uuid[i];
        }
        let gtid = uuid::Uuid::from_bytes(sid);
        let (gno_id, offset) = (readvalue::read_u64(&buf[offset..offset+8]), offset + 8 +1);

        let (last_committed, offset) = (readvalue::read_u64(&buf[offset..offset+8]), offset + 8);
        let (sequence_number, offset) = (readvalue::read_u64(&buf[offset..offset+8]), offset + 8);
        GtidEvent{
            gtid,
            gno_id,
            last_committed,
            sequence_number
        }
    }
}
