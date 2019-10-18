/*
@author: xiao cai niao
@datetime: 2019/9/20
*/

use std::collections::HashMap;
use std::net::TcpStream;
use crate::{io, Config};
//use lazy_static;
//
//lazy_static!{
//    pub static ref mut rollback_traction: Vec<u8> = vec![];
//    pub static ref traction_reset = reset();
//}
//
//fn reset() {
//    rollback_traction = vec![];
//}

pub struct FlagsMeta{
    pub multi_results: i32,
    pub secure_connection: i32,
    pub client_plugin_auth: i32,
    pub client_connect_attrs: i32,
    pub client_plugin_auth_lenenc_client_data: i32,
    pub client_deprecate_eof: i32,
    pub long_password: i32,
    pub long_flag: i32,
    pub protocol_41: i32,
    pub transactions: i32,
    pub client_connect_with_db: i32,
}

impl FlagsMeta {
    pub fn new() -> Self {
        Self {
            multi_results : 1 << 17,
            secure_connection: 1 << 15,
            client_plugin_auth: 1 << 19,
            client_connect_attrs: 1<< 20,
            client_plugin_auth_lenenc_client_data: 1<<21,
            client_deprecate_eof: 1 << 24,
            long_password: 1,
            long_flag: 1 << 2,
            protocol_41: 1 << 9,
            transactions: 1 << 13,
            client_connect_with_db: 9
        }
    }
}

pub enum PackType {
    HandShakeResponse,
    HandShake,
    OkPacket,
    ErrPacket,
    EOFPacket,
    TextResult,
    ComQuery,
    ComQuit,
    ComInitDb,
    ComFieldList,
    ComPrefresh,
    ComStatistics,
    ComProcessInfo,
    ComProcessKill,
    ComDebug,
    ComPing,
    ComChangeUser,
    ComResetConnection,
    ComSetOption,
    ComStmtPrepare,
    ComStmtExecute,
    ComStmtFetch,
    ComStmtClose,
    ComStmtReset,
    ComStmtSendLongData,
}
#[derive(Debug, Clone)]
pub enum  ColumnTypeDict{
    MysqlTypeDecimal,
    MysqlTypeTiny,
    MysqlTypeShort,
    MysqlTypeLong,
    MysqlTypeFloat,
    MysqlTypeDouble,
    MysqlTypeNull,
    MysqlTypeTimestamp,
    MysqlTypeLonglong,
    MysqlTypeInt24,
    MysqlTypeDate,
    MysqlTypeTime,
    MysqlTypeDatetime,
    MysqlTypeYear,
    MysqlTypeNewdate,
    MysqlTypeVarchar,
    MysqlTypeBit,
    MysqlTypeTimestamp2,
    MysqlTypeDatetime2,
    MysqlTypeTime2,
    MysqlTypeJson,
    MysqlTypeNewdecimal,
    MysqlTypeEnum,
    MysqlTypeSet,
    MysqlTypeTinyBlob,
    MysqlTypeMediumBlob,
    MysqlTypeLongBlob,
    MysqlTypeBlob,
    MysqlTypeVarString,
    MysqlTypeString,
    MysqlTypeGeometry,
    UnknowType,
}

impl ColumnTypeDict {
    pub fn from_type_code(typ_code: &u8) -> ColumnTypeDict{
        match typ_code {
            0 => ColumnTypeDict::MysqlTypeDecimal,
            1 => ColumnTypeDict::MysqlTypeTiny,
            2 => ColumnTypeDict::MysqlTypeShort,
            3 => ColumnTypeDict::MysqlTypeLong,
            4 => ColumnTypeDict::MysqlTypeFloat,
            5 => ColumnTypeDict::MysqlTypeDouble,
            6 => ColumnTypeDict::MysqlTypeNull,
            7 => ColumnTypeDict::MysqlTypeTimestamp,
            8 => ColumnTypeDict::MysqlTypeLonglong,
            9 => ColumnTypeDict::MysqlTypeInt24,
            10 => ColumnTypeDict::MysqlTypeDate,
            11 => ColumnTypeDict::MysqlTypeTime,
            12 => ColumnTypeDict::MysqlTypeDatetime,
            13 => ColumnTypeDict::MysqlTypeYear,
            14 => ColumnTypeDict::MysqlTypeNewdate,
            15 => ColumnTypeDict::MysqlTypeVarchar,
            16 => ColumnTypeDict::MysqlTypeBit,
            17 => ColumnTypeDict::MysqlTypeTimestamp2,
            18 => ColumnTypeDict::MysqlTypeDatetime2,
            19 => ColumnTypeDict::MysqlTypeTime2,
            245 => ColumnTypeDict::MysqlTypeJson,
            246 => ColumnTypeDict::MysqlTypeNewdecimal,
            247 => ColumnTypeDict::MysqlTypeEnum,
            248 => ColumnTypeDict::MysqlTypeSet,
            249 => ColumnTypeDict::MysqlTypeTinyBlob,
            250 => ColumnTypeDict::MysqlTypeMediumBlob,
            251 => ColumnTypeDict::MysqlTypeLongBlob,
            252 => ColumnTypeDict::MysqlTypeBlob,
            253 => ColumnTypeDict::MysqlTypeVarString,
            254 => ColumnTypeDict::MysqlTypeString,
            255 => ColumnTypeDict::MysqlTypeGeometry,
            _ => ColumnTypeDict::UnknowType,
        }
    }
}
#[derive(Debug)]
pub enum JsonType{
    NullColumn,
    UnsignedCharColumn,
    UnsignedShortColumn,
    UnsignedInt24Column,
    UnsignedInt64Column,
    UnsignedCharLength,
    UnsignedShortLength,
    UnsignedInt24Length,
    UnsignedInt64Length,
    JsonbTypeSmallObject,
    JsonbTypeLargeObject,
    JsonbTypeSmallArray,
    JsonbTypeLargeArray,
    JsonbTypeLiteral,
    JsonbTypeInt16,
    JsonbTypeUint16,
    JsonbTypeInt32,
    JsonbTypeUint32,
    JsonbTypeInt64,
    JsonbTypeUint64,
    JsonbTypeDouble,
    JsonbTypeString,
    JsonbTypeOpaque,
    JsonbLiteralNull,
    JsonbLiteralTrue,
    JsonbLiteralFalse,

}

impl JsonType {
    pub fn from_type_code(type_code: &usize) -> JsonType{
        match type_code {
            0x0 => JsonType::JsonbTypeSmallObject,
            0x1 => JsonType::JsonbTypeLargeObject,
            0x2 => JsonType::JsonbTypeSmallArray,
            0x3 => JsonType::JsonbTypeLargeArray,
            0x4 => JsonType::JsonbTypeLiteral,
            0x5 => JsonType::JsonbTypeInt16,
            0x6 => JsonType::JsonbTypeUint16,
            0x7 => JsonType::JsonbTypeInt32,
            0x8 => JsonType::JsonbTypeUint32,
            0x9 => JsonType::JsonbTypeInt64,
            0xA => JsonType::JsonbTypeUint64,
            0xB => JsonType::JsonbTypeDouble,
            0xC => JsonType::JsonbTypeString,
            0xF => JsonType::JsonbTypeOpaque,
            _ => JsonType::NullColumn
        }
    }
}

pub fn get_col(conf: &Config, db: &String, tb: &String, table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>) {
    let db_tbl = format!("{}.{}",db,tb);
    match table_cols_info.get(&db_tbl) {
        None => {
            let mut conn = crate::create_conn(conf);
            let sql = format!("select COLUMN_NAME,COLUMN_TYPE,COLUMN_KEY from information_schema.columns where table_schema = '{}' and table_name='{}'  order by ORDINAL_POSITION ;", db, tb);
            let values = io::command::execute(&mut conn,&sql);
            //println!("{:?}",values);
            if values.len() > 0 {
                table_cols_info.insert(db_tbl,values);
            }
        }
        Some(_) => {}
    }
}

pub enum ReadType {
    Repl,
    File
}


