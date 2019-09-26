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

pub struct ColumnTypeDict{
    pub MYSQL_TYPE_DECIMAL: u8,
    pub MYSQL_TYPE_TINY: u8,
    pub MYSQL_TYPE_SHORT: u8,
    pub MYSQL_TYPE_LONG: u8,
    pub MYSQL_TYPE_FLOAT: u8,
    pub MYSQL_TYPE_DOUBLE: u8,
    pub MYSQL_TYPE_NULL: u8,
    pub MYSQL_TYPE_TIMESTAMP: u8,
    pub MYSQL_TYPE_LONGLONG: u8,
    pub MYSQL_TYPE_INT24: u8,
    pub MYSQL_TYPE_DATE: u8,
    pub MYSQL_TYPE_TIME: u8,
    pub MYSQL_TYPE_DATETIME: u8,
    pub MYSQL_TYPE_YEAR: u8,
    pub MYSQL_TYPE_NEWDATE: u8,
    pub MYSQL_TYPE_VARCHAR: u8,
    pub MYSQL_TYPE_BIT: u8,
    pub MYSQL_TYPE_TIMESTAMP2: u8,
    pub MYSQL_TYPE_DATETIME2: u8,
    pub MYSQL_TYPE_TIME2: u8,
    pub MYSQL_TYPE_JSON: u8,
    pub MYSQL_TYPE_NEWDECIMAL: u8,
    pub MYSQL_TYPE_ENUM: u8,
    pub MYSQL_TYPE_SET: u8,
    pub MYSQL_TYPE_TINY_BLOB: u8,
    pub MYSQL_TYPE_MEDIUM_BLOB: u8,
    pub MYSQL_TYPE_LONG_BLOB: u8,
    pub MYSQL_TYPE_BLOB: u8,
    pub MYSQL_TYPE_VAR_STRING: u8,
    pub MYSQL_TYPE_STRING: u8,
    pub MYSQL_TYPE_GEOMETRY: u8
}

impl ColumnTypeDict {
    pub fn new() -> ColumnTypeDict{
        ColumnTypeDict{
            MYSQL_TYPE_DECIMAL: 0,
            MYSQL_TYPE_TINY: 1,
            MYSQL_TYPE_SHORT: 2,
            MYSQL_TYPE_LONG: 3,
            MYSQL_TYPE_FLOAT: 4,
            MYSQL_TYPE_DOUBLE: 5,
            MYSQL_TYPE_NULL: 6,
            MYSQL_TYPE_TIMESTAMP: 7,
            MYSQL_TYPE_LONGLONG: 8,
            MYSQL_TYPE_INT24: 9,
            MYSQL_TYPE_DATE: 10,
            MYSQL_TYPE_TIME: 11,
            MYSQL_TYPE_DATETIME: 12,
            MYSQL_TYPE_YEAR: 13,
            MYSQL_TYPE_NEWDATE: 14,
            MYSQL_TYPE_VARCHAR: 15,
            MYSQL_TYPE_BIT: 16,
            MYSQL_TYPE_TIMESTAMP2: 17,
            MYSQL_TYPE_DATETIME2: 18,
            MYSQL_TYPE_TIME2: 19,
            MYSQL_TYPE_JSON: 245,
            MYSQL_TYPE_NEWDECIMAL: 246,
            MYSQL_TYPE_ENUM: 247,
            MYSQL_TYPE_SET: 248,
            MYSQL_TYPE_TINY_BLOB: 249,
            MYSQL_TYPE_MEDIUM_BLOB: 250,
            MYSQL_TYPE_LONG_BLOB: 251,
            MYSQL_TYPE_BLOB: 252,
            MYSQL_TYPE_VAR_STRING: 253,
            MYSQL_TYPE_STRING: 254,
            MYSQL_TYPE_GEOMETRY: 255,
        }
    }
}

