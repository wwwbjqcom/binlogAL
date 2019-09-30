/*
@author: xiao cai niao
@datetime: 2019/9/27
*/
use serde_json;
use bigdecimal;
use serde::{Serialize,Serializer};
use crate::replication::readevent::{TableMap, EventHeader, BinlogEvent, Tell};
use crate::meta::ColumnTypeDict;
use crate::readvalue;
use crate::replication::jsonb;
use uuid::Error;
use uuid::Version::Mac;
use std::process::id;
use std::fs::metadata;
use bigdecimal::BigDecimal;
use std::io::{Read, Cursor, Seek, SeekFrom};
use byteorder::{ReadBytesExt, BigEndian, LittleEndian};
use std::io;
use std::process;

#[derive(Debug)]
struct DecimalMeta{
    compressed_byte_map: [usize; 10],
    uncompressed_integers: usize,
    uncompressed_decimals: usize,
    compressed_integers: usize,
    compressed_decimals: usize,
    bytes_to_read: usize
}
impl DecimalMeta{
    fn new(precision: u8, decimal: u8) -> DecimalMeta{
        let DECIMAL_DIGITS_PER_INTEGER = 9;
        let compressed_byte_map = [0usize, 1, 1, 2, 2, 3, 3, 4, 4, 4];
        let integral = precision - decimal;
        let uncompressed_integers: usize = (integral / DECIMAL_DIGITS_PER_INTEGER).into();
        let uncompressed_decimals: usize = (decimal / DECIMAL_DIGITS_PER_INTEGER).into();
        let compressed_integers: usize = integral as usize - (uncompressed_integers * DECIMAL_DIGITS_PER_INTEGER as usize);
        let compressed_decimals: usize = decimal as usize - (uncompressed_decimals * DECIMAL_DIGITS_PER_INTEGER as usize);

        let bytes_to_read: usize = uncompressed_integers * 4 + compressed_byte_map[compressed_integers] + uncompressed_decimals * 4 + compressed_byte_map[compressed_decimals];
        DecimalMeta{
            compressed_byte_map,
            uncompressed_integers,
            uncompressed_decimals,
            compressed_integers,
            compressed_decimals,
            bytes_to_read
        }

    }
}

pub trait GetValue{
    fn get_value(&self);
}

#[derive(Debug)]
/// Wrapper for the SQL BLOB (Binary Large OBject) type
///
/// Serializes as Base64
pub struct Blob(Vec<u8>);

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

impl Serialize for Blob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let serialized = base64::encode(&self.0);
        serializer.serialize_str(&serialized)
    }
}

#[derive(Debug)]
pub enum MySQLValue {
    SignedInteger(i64),
    Float(f32),
    Double(f64),
    String(String),
    Enum(i16),
    Blob(Blob),
    Year(u32),
    Date { year: u32, month: u32, day: u32 },
    Time { hours: u32, minutes: u32, seconds: u32, subseconds: u32},
    DateTime { year: u32, month: u32, day: u32, hour: u32, minute: u32, second: u32, subsecond: u32 },
    Json(serde_json::Value),
    Decimal(bigdecimal::BigDecimal),
    Timestamp { unix_time: i32, subsecond: u32 },
    Null
}


/*
fixed_part: 10bytes
    table_id: 6bytes
    reserved: 2bytes
    extra: 2bytes
variable_part:
    columns: 1bytes
    variable_sized: int((n+7)/8) n=columns.value
    variable_sized: int((n+7)/8) (for updata_row_event only)

    variable_sized: int((n+7)/8)
    row_value : variable size

    crc : 4bytes

The The data first length of the varchar type more than 255 are 2 bytes
*/

#[derive(Debug)]
pub struct RowValue{
    rows: Vec<Vec<Option<MySQLValue>>>,
}

fn is_null(null_bytes: &Vec<u8>, pos: &usize) -> u8 {
    let idx = (pos / 8) as usize;
    let bit = null_bytes[idx];
    return bit & (1 << (pos % 8));
}
impl RowValue{
    pub fn read_row_value<R: Read+Seek>(buf: &mut R, map: &TableMap, header: &EventHeader) -> RowValue {
        let row_event_fix = 10;
        buf.seek(io::SeekFrom::Current(row_event_fix));
        let col_count = buf.read_u8().unwrap();

        let columns_length = ((col_count + 7) / 8) as i64;
        match header.type_code {
            BinlogEvent::UpdateEvent => {
                buf.seek(io::SeekFrom::Current(columns_length * 2));
            }
            _ => {
                buf.seek(io::SeekFrom::Current(columns_length));

            }
        }

        let mut rows: Vec<Vec<Option<MySQLValue>>> = vec![];;
        loop {
            let mut null_bit = vec![0u8; columns_length as usize];
            buf.read_exact(&mut null_bit);

            let mut row: Vec<Option<MySQLValue>> = vec![];
            let columns = map.column_info.len();
            for idx in (0..columns) {
                let value= if is_null(&null_bit.to_vec(), &idx) > 0{
                    MySQLValue::Null
                } else {
                    Self::parsevalue(buf, &map.column_info[idx].column_type, &map.column_info[idx].column_meta)


                };
                row.push(Some(value));
            }
            rows.push(row);
            if (buf.tell().unwrap() + 4) as usize > header.event_length as usize {
                break;
            }
        };

        RowValue{
            rows
        }
    }

    fn parsevalue<R: Read>(buf: &mut R, type_code: &ColumnTypeDict, col_meta: &Vec<usize>) -> MySQLValue{
        match type_code {
            ColumnTypeDict::MYSQL_TYPE_TINY => {
                MySQLValue::SignedInteger(buf.read_i8().unwrap() as i64)
            }
            ColumnTypeDict::MYSQL_TYPE_SHORT => {
                MySQLValue::SignedInteger(buf.read_i16::<LittleEndian>().unwrap() as i64)
            }
            ColumnTypeDict::MYSQL_TYPE_INT24 => {
                MySQLValue::SignedInteger(buf.read_i24::<LittleEndian>().unwrap() as i64)
            }
            ColumnTypeDict::MYSQL_TYPE_LONG => {
                MySQLValue::SignedInteger(buf.read_i32::<LittleEndian>().unwrap() as i64)
            }
            ColumnTypeDict::MYSQL_TYPE_LONGLONG => {
                MySQLValue::SignedInteger(buf.read_i64::<LittleEndian>().unwrap() as i64)
            }
            ColumnTypeDict::MYSQL_TYPE_NEWDECIMAL => {
                let decimal_meta = DecimalMeta::new(col_meta[0] as u8, col_meta[1] as u8);
                let mut value_buf = vec![0u8; decimal_meta.bytes_to_read];
                buf.read_exact(&mut value_buf);
                match Self::read_new_decimal(&value_buf.to_vec(), &decimal_meta) {
                    Ok(T) => MySQLValue::Decimal(T),
                    Err(e) => {
                        println!("decimal 解析错误: {}",e);
                        MySQLValue::Null
                    }
                }
            }
            ColumnTypeDict::MYSQL_TYPE_DOUBLE |
            ColumnTypeDict::MYSQL_TYPE_FLOAT => {
                match col_meta[0] {
                    8 => MySQLValue::Double(buf.read_f64::<LittleEndian>().unwrap() as f64),
                    4 => MySQLValue::Float(buf.read_f32::<LittleEndian>().unwrap() as f32),
                    _ => MySQLValue::Null
                }
            }
            ColumnTypeDict::MYSQL_TYPE_TIMESTAMP2 => {
                let whole_part = buf.read_i32::<BigEndian>().unwrap();
                let frac_part = Self::read_datetime_fsp(buf, col_meta[0] as u8).unwrap();
                MySQLValue::Timestamp { unix_time: whole_part, subsecond: frac_part }
            }
            ColumnTypeDict::MYSQL_TYPE_DATETIME2 => {
                /*
                DATETIME
                1 bit  sign           (1= non-negative, 0= negative)
                17 bits year*13+month  (year 0-9999, month 0-12)
                 5 bits day            (0-31)
                 5 bits hour           (0-23)
                 6 bits minute         (0-59)
                 6 bits second         (0-59)
                ---------------------------
                40 bits = 5 bytes
                */
                let mut tmp_buf = [0u8; 5];
                buf.read_exact(&mut tmp_buf);
                let subsecond = Self::read_datetime_fsp(buf, col_meta[0] as u8).unwrap();
                tmp_buf[0] &= 0x7f;

                let year_month: u32 = ((tmp_buf[2] as u32) >> 6) + ((tmp_buf[1] as u32) << 2) + ((tmp_buf[0] as u32) << 10);
                let year = year_month / 13;
                let month = year_month % 13;

                let day = ((tmp_buf[2] & 0x3e) as u32) >> 1;

                let hour = (((tmp_buf[3] & 0xf0) as u32) >> 4) + (((tmp_buf[2] & 0x01) as u32) << 4);
                let minute = (tmp_buf[4] >> 6) as u32 + (((tmp_buf[3] & 0x0f) as u32) << 2);
                let second = (tmp_buf[4] & 0x3f) as u32;
                MySQLValue::DateTime { year, month, day, hour, minute, second, subsecond }
            }
            ColumnTypeDict::MYSQL_TYPE_YEAR => {
                MySQLValue::Year(buf.read_u8().unwrap() as u32 + 1900)
            }
            ColumnTypeDict::MYSQL_TYPE_DATE => {
                let value = buf.read_u24::<LittleEndian>().unwrap();
                let year = (value & ((1 << 15) - 1) << 9) >> 9;
                let month = (value & ((1 << 4) - 1) << 5) >> 5;
                let day = (value & ((1 << 5) - 1));
                if year == 0 {MySQLValue::Null}
                else if month == 0 { MySQLValue::Null }
                else if day == 0 { MySQLValue::Null }
                else { MySQLValue::Date {year, month, day} }

            }
            ColumnTypeDict::MYSQL_TYPE_TIME2 => {
                /*
                TIME encoding for nonfractional part:

                 1 bit sign    (1= non-negative, 0= negative)
                 1 bit unused  (reserved for future extensions)
                10 bits hour   (0-838)
                 6 bits minute (0-59)
                 6 bits second (0-59)
                ---------------------
                24 bits = 3 bytes
                */
                let mut tmp_buf = [0u8; 3];
                buf.read_exact(&mut tmp_buf);
                let hours = (((tmp_buf[0] & 0x3f) as u32) << 4) | (((tmp_buf[1] & 0xf0) as u32) >> 4);
                let minutes = (((tmp_buf[1] & 0x0f) as u32) << 2) | (((tmp_buf[2] & 0xb0) as u32) >> 6);
                let seconds = (tmp_buf[2] & 0x3f) as u32;
                let frac_part = Self::read_datetime_fsp(buf, col_meta[0] as u8).unwrap();
                MySQLValue::Time { hours, minutes, seconds, subseconds: frac_part }
            }
            ColumnTypeDict::MYSQL_TYPE_VARCHAR |
            ColumnTypeDict::MYSQL_TYPE_VAR_STRING |
            ColumnTypeDict::MYSQL_TYPE_BLOB |
            ColumnTypeDict::MYSQL_TYPE_TINY_BLOB |
            ColumnTypeDict::MYSQL_TYPE_LONG_BLOB |
            ColumnTypeDict::MYSQL_TYPE_MEDIUM_BLOB |
            ColumnTypeDict::MYSQL_TYPE_SET |
            ColumnTypeDict::MYSQL_TYPE_BIT => {
                let var_length =  Self::read_str_value_length(buf, &col_meta[0]);
                let mut pack = vec![0u8; var_length];
                buf.read_exact(&mut pack);
                MySQLValue::Blob(Blob::from(pack.to_vec()))
            }
            ColumnTypeDict::MYSQL_TYPE_JSON => {
                let value_length = Self::read_str_value_length(buf, &col_meta[0]);
                MySQLValue::Json(jsonb::read_binary_json(buf, &value_length))

            }
            ColumnTypeDict::MYSQL_TYPE_STRING => {
                let mut value_length = 0;
                if col_meta[0] <= 255 {
                    value_length = buf.read_u8().unwrap() as usize;
                }
                else {
                    value_length = buf.read_u16::<LittleEndian>().unwrap() as usize;
                }
                let value = readvalue::read_string_value_from_len(buf,value_length);
                MySQLValue::String(value)
            }
            ColumnTypeDict::MYSQL_TYPE_ENUM => {
                match col_meta[0] {
                    1 => {
                        let v = buf.read_u8().unwrap();
                        MySQLValue::SignedInteger(v as i64)
                    },
                    2 => {
                        let v = buf.read_u16::<LittleEndian>().unwrap();
                        MySQLValue::SignedInteger(v as i64)
                    }
                    _ => MySQLValue::Null
                }
            }
            _ => MySQLValue::Null
        }
    }

    fn read_str_value_length<R: Read>(buf: &mut R, meta: &usize) -> usize {
        match meta {
            1 => buf.read_u8().unwrap() as usize,
            2 => buf.read_u16::<LittleEndian>().unwrap() as usize,
            3 => buf.read_u24::<LittleEndian>().unwrap() as usize,
            4 => buf.read_u32::<LittleEndian>().unwrap() as usize,
            5 => {
                let mut pack = [0u8; 5];
                buf.read_exact(&mut pack);
                readvalue::read_u40(&pack) as usize
            }
            6 => {
                let mut pack = [0u8; 6];
                buf.read_exact(&mut pack);
                readvalue::read_u48(&pack) as usize
            }
            7 => {
                let mut pack = [0u8; 7];
                buf.read_exact(&mut pack);
                readvalue::read_u56(&pack) as usize
            }
            8 => {
                let mut pack = [0u8; 8];
                buf.read_exact(&mut pack);
                readvalue::read_u64(&pack) as usize
            }
            _ => 0 as usize
        }
    }

    fn read_new_decimal(buf: &Vec<u8>, meta: &DecimalMeta) -> Result<BigDecimal, failure::Error> {
        let mut components = Vec::new();
        let mut buf = buf.clone();
        let is_negative = (buf[0] & 0x80) == 0;
        let mut mask = 0;
        buf[0] ^= 0x80;
        if is_negative {
            components.push("-".to_owned());
            mask = -1;
        }

        let mut r = Cursor::new(buf);
        if meta.compressed_integers != 0 {
            let to_read = meta.compressed_byte_map[meta.compressed_integers];
            let v = Self::read_int_be_by_size(&mut r, to_read)? ^ mask;
            components.push(v.to_string())
        }
        for _ in 0..meta.uncompressed_integers {
            let v = r.read_i32::<BigEndian>()? ^ mask as i32;
            components.push(format!("{:09}", v));
        }
        components.push(".".to_owned());
        for _ in 0..meta.uncompressed_decimals {
            let v =  r.read_u32::<BigEndian>()? ^ mask as u32;
            components.push(format!("{:09}", v));
        }
        if meta.compressed_decimals != 0 {
            let v = Self::read_int_be_by_size(&mut r, meta.compressed_byte_map[meta.compressed_decimals])? ^ mask;
            components.push(format!("{:.*}",meta.compressed_decimals,v as usize));
        }
        components.join("").parse::<BigDecimal>().map_err(|e| failure::Error::from_boxed_compat(Box::new(e)))
    }

    fn read_int_be_by_size<R: Read>(r: &mut R, bytes: usize) -> io::Result<i64> {
        Ok(match bytes {
            1 => i64::from(r.read_i8()?),
            2 => i64::from(r.read_i16::<BigEndian>()?),
            3 => {
                let mut buf = [0u8; 3];
                r.read_exact(&mut buf)?;
                let (a, b, c) = (buf[0] as i64, buf[1] as i64, buf[2] as i64);
                let mut res = (a << 16) | (b << 8) | c;
                if res >= 0x800000 {
                    res -= 0x1000000;
                }
                res
            },
            4 => i64::from(r.read_i32::<BigEndian>()?),
            _ => unimplemented!(),
        })
    }


    fn read_datetime_fsp<R: Read>(r: &mut R, column: u8) -> io::Result<u32> {
        Ok(match column {
            0 => 0u32,
            1 | 2 => Self::read_int_be_by_size(r, 1)? as u32,
            3 | 4 => Self::read_int_be_by_size(r, 2)? as u32,
            5 | 6 => Self::read_int_be_by_size(r, 3)? as u32,
            _ => 0u32,
        })
    }
}


