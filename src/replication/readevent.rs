/*
@author: xiao cai niao
@datetime: 2019/9/25
*/
use crate::{readvalue, Config};
use std::{io};
use uuid;
use uuid::Uuid;
use std::io::{Read, Seek, SeekFrom, Result};
use crate::meta::ColumnTypeDict;
use byteorder::{ReadBytesExt, LittleEndian};
use std::alloc::handle_alloc_error;


pub trait Tell: Seek {
    fn tell(&mut self) -> Result<u64> {
        self.seek(SeekFrom::Current(0))
    }
}

impl<T> Tell for T where T: Seek { }


#[derive(Debug, Clone)]
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
    FormatDescriptionEvent,
    UNKNOWNEVENT,
    PreviousGtidsLogEvent,
    CreateFileEvent
}

pub trait InitHeader{
    fn new<R: Read+Seek>(buf: &mut R, conf: &Config) -> Self;
}

pub trait InitValue{
    fn read_event<R: Read+Seek>(header: &EventHeader, buf: &mut R, version: &u8) -> Self;
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
#[derive(Debug, Clone)]
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
    fn new<R: Read + Seek>(buf: &mut R, conf: &Config) -> EventHeader{
        let mut header_length: u8 = 19;
        if conf.runtype == String::from("repl"){
            //如果是模拟slave同步会多1字节的头部分
            buf.seek(io::SeekFrom::Current(1)).unwrap();
            header_length += 1;
        }
        let timestamp = buf.read_u32::<LittleEndian>().unwrap();
        let type_code = Self::get_type_code_event(&Some(buf.read_u8().unwrap() as u8));
        let server_id = buf.read_u32::<LittleEndian>().unwrap();
        let event_length = buf.read_u32::<LittleEndian>().unwrap();
        let next_position = buf.read_u32::<LittleEndian>().unwrap();
        let flags = buf.read_u16::<LittleEndian>().unwrap();
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
            Some(16) => BinlogEvent::XidEvent,
            Some(38) => BinlogEvent::XAPREPARELOGEVENT,
            Some(15) => BinlogEvent::FormatDescriptionEvent,
            Some(35) => BinlogEvent::PreviousGtidsLogEvent,
            Some(8) => BinlogEvent::CreateFileEvent,
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
#[derive(Debug, Clone)]
pub struct QueryEvent{
    pub thread_id: u32,
    pub execute_seconds: u32,
    pub database: String,
    pub command: String
}

impl InitValue for QueryEvent{
    fn read_event<R: Read+Seek>(header: &EventHeader, buf: &mut R, version: &u8) -> QueryEvent{
        let thread_id = buf.read_u32::<LittleEndian>().unwrap();
        let execute_seconds = buf.read_u32::<LittleEndian>().unwrap();
        let database_length = buf.read_u8().unwrap();
        let _error_code = buf.read_u16::<LittleEndian>().unwrap();
        let variable_block_length = buf.read_u16::<LittleEndian>().unwrap();
        buf.seek(io::SeekFrom::Current(variable_block_length as i64)).unwrap();
        let mut database_pack = vec![0u8; database_length as usize];
        buf.read_exact(&mut database_pack).unwrap();
        let database = readvalue::read_string_value(&database_pack);
        buf.seek(io::SeekFrom::Current(1)).unwrap();

        let mut command_pak = vec![];
        let mut command = String::from("");
        if *version == 5 {
            let command_length = header.event_length as usize - header.header_length as usize - buf.tell().unwrap() as usize - 4;
            command_pak = vec![0u8; command_length];
            buf.read_exact(&mut command_pak).unwrap();
            command = readvalue::read_string_value(&command_pak);
        }else {
            buf.read_to_end(&mut command_pak).unwrap();
            command = readvalue::read_string_lossy_value(&command_pak, version);
        }

        QueryEvent{
            thread_id,
            execute_seconds,
            database,
            command
        }

    }
}

#[derive(Debug, Clone)]
pub struct XidEvent{
    pub xid: u64
}

impl InitValue for XidEvent{
    fn read_event<R: Read>(_header: &EventHeader, buf: &mut R, _version: &u8) -> XidEvent{
        let xid = buf.read_u64::<LittleEndian>().unwrap();
        XidEvent{
            xid
        }
    }
}

/*
rotate_log_event:
    Fixed data part: 8bytes
    Variable data part: event_length - header_length - fixed_length (string<EOF>)
*/
#[derive(Debug, Clone)]
pub struct RotateLog{
    pub binlog_file: String
}

impl InitValue for RotateLog{
    fn read_event<R: Read+Seek>(header: &EventHeader, buf: &mut R, version: &u8) -> RotateLog{
        let mut offset = 8 as usize;
        if version == &5{
            offset += 4;
        }
        buf.seek(io::SeekFrom::Current(8)).unwrap();
        let len_gg = header.event_length as usize - header.header_length as usize - offset;
        let mut tmp_buf = vec![0u8; len_gg];
        buf.read_exact(&mut tmp_buf).unwrap();
        let binlog_file = String::from_utf8_lossy(&tmp_buf).to_string();
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
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub column_type: ColumnTypeDict,
    pub column_meta: Vec<usize>
}

#[derive(Debug, Clone)]
pub struct TableMap{
    pub database_name: String,
    pub table_name: String,
    pub column_count: u8,
    pub column_info: Vec<ColumnInfo>,
}
impl TableMap{
    pub fn new() -> TableMap {
        TableMap{
            database_name: "".to_string(),
            table_name: "".to_string(),
            column_count: 0,
            column_info: vec![]
        }
    }

    fn read_column_meta<R: Read>(buf: &mut R,col_type: &u8) -> (Vec<usize>, u8) {
        let mut value: Vec<usize> = vec![];
        //let mut offset = offset;
        let mut col_type = col_type.clone();
        let column_type_info = ColumnTypeDict::from_type_code(&col_type);
        match column_type_info {
            ColumnTypeDict::MysqlTypeVarString => {
                value = Self::read_string_meta(buf);
            }
            ColumnTypeDict::MysqlTypeVarchar => {
                value = Self::read_string_meta(buf);
            }
            ColumnTypeDict::MysqlTypeBlob => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeMediumBlob => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeLongBlob => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeTinyBlob => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeJson => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeTimestamp2 => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeDatetime2 => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeTime2 => {
                //value = vec![buf[offset] as usize];
                //offset += 1;
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeNewdecimal => {
                value.extend(Self::read_newdecimal(buf).to_owned().to_vec());
            }
            ColumnTypeDict::MysqlTypeFloat => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeDouble => {
                value = Self::read_one_bytes(buf);
            }
            ColumnTypeDict::MysqlTypeString => {
                let (a, b) = Self::read_string_type(buf);
                value = a;
                col_type = b;
            }
            _ => {
                value = vec![0];
            }
        }
        return (value, col_type);
    }

    fn read_one_bytes<R: Read>(buf: &mut R) -> Vec<usize> {
        let v = buf.read_u8().unwrap() as usize;
        vec![v]
    }


    fn read_string_meta<R: Read>(buf: &mut R) -> Vec<usize> {
        let metadata = buf.read_u16::<LittleEndian>().unwrap();
        let mut v = vec![];
        if metadata > 255 {
            v.push(2);
        }else {
            v.push(1);
        }
        v
    }

    fn read_newdecimal<R: Read>(buf: &mut R) -> [usize;2] {
        let precision = buf.read_u8().unwrap() as usize;
        let decimals = buf.read_u8().unwrap() as usize;
        [precision,decimals]
    }

    fn read_string_type<R: Read>(buf: &mut R) -> (Vec<usize>, u8) {
        let _type = buf.read_u8().unwrap();
        let code = ColumnTypeDict::from_type_code(&_type);

        let metadata = buf.read_u8().unwrap() as usize;
        match code {
            ColumnTypeDict::MysqlTypeEnum |
            ColumnTypeDict::MysqlTypeSet |
            ColumnTypeDict::MysqlTypeString => {
                return (vec! [metadata], _type);
            }
            ColumnTypeDict::UnknowType => {
                return (vec! [65535], 254);
            }
            _ => {
                return (vec! [65535], 254);
            }
        }
    }

}

impl InitValue for TableMap{
    fn read_event<R: Read+Seek>( _header: &EventHeader,buf: &mut R, _version: &u8) -> TableMap{
        buf.seek(io::SeekFrom::Current(8)).unwrap();
        let database_length = buf.read_u8().unwrap() as usize;
        let database_name = readvalue::read_string_value_from_len(buf, database_length);
        buf.seek(io::SeekFrom::Current(1)).unwrap();
        let table_length = buf.read_u8().unwrap() as usize;
        let table_name = readvalue::read_string_value_from_len(buf, table_length);
        buf.seek(io::SeekFrom::Current(1)).unwrap();

        let column_count = buf.read_u8().unwrap();
        let mut column_info: Vec<ColumnInfo> = vec![];
        let mut column_type_list = vec![0u8; column_count as usize];
        buf.read_exact(&mut column_type_list).unwrap();
        buf.seek(io::SeekFrom::Current(1)).unwrap(); //跳过mmetadata_lenth,直接用字段数据进行判断
        for col_type in column_type_list.iter() {
            let (col_meta, col_type) = Self::read_column_meta(buf, col_type);
            column_info.push(ColumnInfo{column_type: ColumnTypeDict::from_type_code(&col_type),column_meta: col_meta});
        }


        TableMap{
            database_name,
            table_name,
            column_count,
            column_info
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

#[derive(Debug, Clone)]
pub struct GtidEvent{
    pub gtid: Uuid,
    pub gno_id: u64,
    pub last_committed: u64,
    pub sequence_number: u64
}

impl InitValue for GtidEvent {
    fn read_event<R: Read+Seek>(_header: &EventHeader, buf: &mut R, _version: &u8) -> GtidEvent {
        buf.seek(io::SeekFrom::Current(1)).unwrap();
        let mut sid = [0 as u8; 16];
        buf.read_exact(&mut sid).unwrap();

        let gtid = uuid::Uuid::from_bytes(sid);
        let gno_id = buf.read_u64::<LittleEndian>().unwrap();

        let last_committed = buf.read_u64::<LittleEndian>().unwrap();
        let sequence_number = buf.read_u64::<LittleEndian>().unwrap();

        GtidEvent{
            gtid,
            gno_id,
            last_committed,
            sequence_number
        }
    }
}
