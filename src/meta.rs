/*
@author: xiao cai niao
@datetime: 2019/9/20
*/

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
#[derive(Debug)]
pub enum  ColumnTypeDict{
    MYSQL_TYPE_DECIMAL,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE,
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_JSON,
    MYSQL_TYPE_NEWDECIMAL,
    MYSQL_TYPE_ENUM,
    MYSQL_TYPE_SET,
    MYSQL_TYPE_TINY_BLOB,
    MYSQL_TYPE_MEDIUM_BLOB,
    MYSQL_TYPE_LONG_BLOB,
    MYSQL_TYPE_BLOB,
    MYSQL_TYPE_VAR_STRING,
    MYSQL_TYPE_STRING,
    MYSQL_TYPE_GEOMETRY,
    UNKNOW_TYPE,
}

impl ColumnTypeDict {
    pub fn from_type_code(typ_code: &u8) -> ColumnTypeDict{
        match typ_code {
            0 => ColumnTypeDict::MYSQL_TYPE_DECIMAL,
            1 => ColumnTypeDict::MYSQL_TYPE_TINY,
            2 => ColumnTypeDict::MYSQL_TYPE_SHORT,
            3 => ColumnTypeDict::MYSQL_TYPE_LONG,
            4 => ColumnTypeDict::MYSQL_TYPE_FLOAT,
            5 => ColumnTypeDict::MYSQL_TYPE_DOUBLE,
            6 => ColumnTypeDict::MYSQL_TYPE_NULL,
            7 => ColumnTypeDict::MYSQL_TYPE_TIMESTAMP,
            8 => ColumnTypeDict::MYSQL_TYPE_LONGLONG,
            9 => ColumnTypeDict::MYSQL_TYPE_INT24,
            10 => ColumnTypeDict::MYSQL_TYPE_DATE,
            11 => ColumnTypeDict::MYSQL_TYPE_TIME,
            12 => ColumnTypeDict::MYSQL_TYPE_DATETIME,
            13 => ColumnTypeDict::MYSQL_TYPE_YEAR,
            14 => ColumnTypeDict::MYSQL_TYPE_NEWDATE,
            15 => ColumnTypeDict::MYSQL_TYPE_VARCHAR,
            16 => ColumnTypeDict::MYSQL_TYPE_BIT,
            17 => ColumnTypeDict::MYSQL_TYPE_TIMESTAMP2,
            18 => ColumnTypeDict::MYSQL_TYPE_DATETIME2,
            19 => ColumnTypeDict::MYSQL_TYPE_TIME2,
            245 => ColumnTypeDict::MYSQL_TYPE_JSON,
            246 => ColumnTypeDict::MYSQL_TYPE_NEWDECIMAL,
            247 => ColumnTypeDict::MYSQL_TYPE_ENUM,
            248 => ColumnTypeDict::MYSQL_TYPE_SET,
            249 => ColumnTypeDict::MYSQL_TYPE_TINY_BLOB,
            250 => ColumnTypeDict::MYSQL_TYPE_MEDIUM_BLOB,
            251 => ColumnTypeDict::MYSQL_TYPE_LONG_BLOB,
            252 => ColumnTypeDict::MYSQL_TYPE_BLOB,
            253 => ColumnTypeDict::MYSQL_TYPE_VAR_STRING,
            254 => ColumnTypeDict::MYSQL_TYPE_STRING,
            255 => ColumnTypeDict::MYSQL_TYPE_GEOMETRY,
            _ => ColumnTypeDict::UNKNOW_TYPE,
        }
    }
}
#[derive(Debug)]
pub enum JsonType{
    NULL_COLUMN,
    UNSIGNED_CHAR_COLUMN,
    UNSIGNED_SHORT_COLUMN,
    UNSIGNED_INT24_COLUMN,
    UNSIGNED_INT64_COLUMN,
    UNSIGNED_CHAR_LENGTH,
    UNSIGNED_SHORT_LENGTH,
    UNSIGNED_INT24_LENGTH,
    UNSIGNED_INT64_LENGTH,
    JSONB_TYPE_SMALL_OBJECT,
    JSONB_TYPE_LARGE_OBJECT,
    JSONB_TYPE_SMALL_ARRAY,
    JSONB_TYPE_LARGE_ARRAY,
    JSONB_TYPE_LITERAL,
    JSONB_TYPE_INT16,
    JSONB_TYPE_UINT16,
    JSONB_TYPE_INT32,
    JSONB_TYPE_UINT32,
    JSONB_TYPE_INT64,
    JSONB_TYPE_UINT64,
    JSONB_TYPE_DOUBLE,
    JSONB_TYPE_STRING,
    JSONB_TYPE_OPAQUE,
    JSONB_LITERAL_NULL,
    JSONB_LITERAL_TRUE,
    JSONB_LITERAL_FALSE,

}

impl JsonType {
    pub fn from_type_code(type_code: &usize) -> JsonType{
        match type_code {
            0x0 => JsonType::JSONB_TYPE_SMALL_OBJECT,
            0x1 => JsonType::JSONB_TYPE_LARGE_OBJECT,
            0x2 => JsonType::JSONB_TYPE_SMALL_ARRAY,
            0x3 => JsonType::JSONB_TYPE_LARGE_ARRAY,
            0x4 => JsonType::JSONB_TYPE_LITERAL,
            0x5 => JsonType::JSONB_TYPE_INT16,
            0x6 => JsonType::JSONB_TYPE_UINT16,
            0x7 => JsonType::JSONB_TYPE_INT32,
            0x8 => JsonType::JSONB_TYPE_UINT32,
            0x9 => JsonType::JSONB_TYPE_INT64,
            0xA => JsonType::JSONB_TYPE_UINT64,
            0xB => JsonType::JSONB_TYPE_DOUBLE,
            0xC => JsonType::JSONB_TYPE_STRING,
            0xF => JsonType::JSONB_TYPE_OPAQUE,
            _ => JsonType::NULL_COLUMN
        }
    }
}



