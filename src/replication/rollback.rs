/*
@author: xiao cai niao
@datetime: 2019/10/17
*/


use std::fs::File;
use std::io::{Seek, SeekFrom, Write, Cursor, Read, BufReader};
use crate::replication::readevent::{EventHeader, BinlogEvent, TableMap, Tell, InitHeader};
use byteorder::{ReadBytesExt, LittleEndian};
use crate::meta::ColumnTypeDict;
use crate::{readvalue, Config};
use std::env::set_var;
use crate::replication::readevent;


#[derive(Debug)]
pub struct RollBackTrac{
    pub desc_event: Vec<u8>,
    pub count: usize,
    pub events: Vec<Vec<u8>>,
    pub file_seq: usize,
    pub desc_format: Vec<u8>,
    pub cur_event: Vec<u8>,
    pub rollback_traction: Vec<u8>,
    pub rfilesize: usize,
    pub rollback: bool,
}
impl RollBackTrac {
    pub fn new(reader: &mut BufReader<File>,conf: & Config) -> RollBackTrac{
        let mut desc_format = vec![];
        let mut rfilesize = 0;
        let mut rollback = false;
        if conf.rollback {
            rollback = true;
            desc_format = Self::get_desc_format_event(reader,conf);
            if conf.rfilesize.len()> 0 {
                rfilesize = conf.rfilesize.parse().unwrap();
            }
        }
        else {
            reader.seek(SeekFrom::Start(4));
        }
        if conf.startposition.len() > 0 {
            reader.seek(SeekFrom::Start(conf.startposition.parse().unwrap()));
        }
        RollBackTrac{
            desc_event: vec![],
            count: 0,
            events: vec![],
            file_seq: 1,
            desc_format,
            cur_event: vec![],
            rollback_traction: vec![],
            rfilesize,
            rollback
        }
    }

    fn get_desc_format_event(reader: &mut BufReader<File>,conf: &Config) -> Vec<u8> {
        reader.seek(SeekFrom::Start(4));
        let mut desc_format: Vec<u8> = vec![];
        let mut header_buf = vec![0u8; 19];
        reader.read_exact(header_buf.as_mut()).unwrap();
        desc_format.extend(&header_buf);
        let mut cur = Cursor::new(header_buf);
        let event_header: EventHeader = readevent::EventHeader::new(&mut cur,conf);
        let payload = event_header.event_length as usize - event_header.header_length as usize;
        let mut payload_buf = vec![0u8; payload];
        reader.read_exact(payload_buf.as_mut()).unwrap();
        desc_format.extend(&payload_buf);
        desc_format
    }

    pub fn get_rollbak_file(files_seq: usize) -> File {
        let mut write_file: File;
        let name = format!("rollback-{}.log",files_seq);
        write_file = File::create(name).unwrap_or_else(|err|{
            println!("{}",err);
            std::process::exit(1);
        });
        write_file
    }

    //倒叙写入事务信息
    pub fn write_rollback_log(&mut self) -> std::io::Result<()> {
        let mut write_file = Self::get_rollbak_file(self.file_seq);
        let a = [0xFE,0x62,0x69,0x6E];
        write_file.write_all(&a)?;
        write_file.write_all(&self.desc_format);
        let tracs = self.events.len();
        let count = self.count;
        let mut a = vec![0; tracs];
        for i in 0..tracs {
            a[i] = i;
        }
        a.reverse();
        for i in a {
            write_file.write_all(&self.events[i])?;
        }
        Ok(())
    }

    //判断
    pub fn check_file_size(&mut self) -> bool {
        let mut file_size = 1024 * 1024 * 1024;
        if self.rfilesize > 0{
            file_size = self.rfilesize;
        }
        if self.count > file_size {
            //println!("{},{}",self.count, file_size);
            self.write_rollback_log();
            return true;
        }else {
            return false;
        }
    }

    pub fn is_write(&mut self){
        if self.rollback{
            self.write_rollback_log();
        }
    }

    pub fn append_cur_event(&mut self, buf: &Vec<u8>) {
        if self.rollback{
            self.cur_event.extend(buf);
        }
    }

    pub fn update(&mut self) {
        self.file_seq = self.file_seq + 1;
        self.count = 0;
        self.rollback_traction = vec![];
        self.events = vec![];
    }

    pub fn update_event(&mut self) {
        if self.rollback{
            self.rollback_traction = vec![];
            self.cur_event = vec![];
        }
    }

    pub fn init_traction_buf(&mut self) {
        if self.rollback{
            self.rollback_traction = vec![];
        }
    }

    pub fn delete_cur_event(&mut self){
        if self.rollback{
            self.cur_event = vec![];
        }
    }
}



pub fn rollback_row_event(event: &Vec<u8>, event_header: &EventHeader, map: &TableMap) -> Vec<u8> {
    let mut new_row_event: Vec<u8> = event.clone();
    match event_header.type_code {
        BinlogEvent::UpdateEvent => {
            let mut cur = Cursor::new(new_row_event);
            update_event(&mut cur, map, event_header)
        }
        BinlogEvent::DeleteEvent => {
            new_row_event[4] = 30;
            new_row_event
        }
        BinlogEvent::WriteEvent => {
            new_row_event[4] = 32;
            new_row_event
        }
        _ => {
            event.clone()
        }
    }
}

fn tmp_var() -> (Vec<u8>, Vec<u8>){
    (vec![],vec![])
}

fn update_event<R: Read+Seek>(event: &mut R, map: &TableMap, event_header: &EventHeader) -> Vec<u8> {
    let mut new_row_event: Vec<u8> = vec![];
    let mut header = [0u8; 19];
    event.read_exact(&mut header).unwrap();
    new_row_event.extend(&header);

    let mut fix_buf = [0u8; 8];
    event.read_exact(&mut fix_buf).unwrap();
    new_row_event.extend(&fix_buf);

    let mut extra = [0u8; 2];
    event.read_exact(&mut extra).unwrap();
    new_row_event.extend(&extra);
    let a = crate::readvalue::read_u16(&extra);

    if a > 2{
        let tmp = a -2;
        let mut extra_o = vec![0u8; tmp as usize];
        event.read_exact(&mut extra_o).unwrap();
        new_row_event.extend(&extra_o);
    }

    let cols = event.read_u8().unwrap();
    new_row_event.push(cols);

    let cols_var = ((cols + 7) / 8) as usize ;
    let mut vars = vec![0u8; (cols_var * 2) as usize];
    event.read_exact(&mut vars).unwrap();
    new_row_event.extend(&vars);
    let (mut before_bytes , mut after_bytes)= tmp_var();
    loop {
        let mut nulls = vec![0u8; cols_var];
        event.read_exact(&mut nulls).unwrap();
        //new_row_event.extend(&nulls);

        let columns = map.column_info.len();
        let mut row_bytes: Vec<u8> = vec![];
        for idx in 0..columns {
            if crate::replication::parsevalue::is_null(&nulls, &idx) > 0{

            } else {
                let col_bytes = parese_row_bytes(event, &map.column_info[idx].column_type, &map.column_info[idx].column_meta);
                row_bytes.extend(col_bytes);
            }
        }

        if before_bytes.len() == 0 {
            before_bytes.extend(nulls);
            before_bytes.extend(row_bytes);
        }else {
            after_bytes.extend(nulls);
            after_bytes.extend(row_bytes);
            new_row_event.extend(after_bytes);
            new_row_event.extend(before_bytes);
            before_bytes = vec![];
            after_bytes = vec![];
        }

        if (event.tell().unwrap() + 4) as usize >= event_header.event_length as usize {
            let mut a = vec![];
            event.read_to_end(&mut a);
            if a.len() > 0 {
                new_row_event.extend(a);
            }

            break;
        }
    }

    new_row_event
}


fn parese_row_bytes<R: Read + Tell>(buf: &mut R, type_code: &ColumnTypeDict, col_meta: &Vec<usize>) -> Vec<u8> {
    let mut row_bytes= vec![];
    let mut tmp = vec![];
    match type_code {
        ColumnTypeDict::MysqlTypeTiny => {
            tmp = vec![0u8; 1];
            //row_bytes.push(buf.read(row_bytes.as_mut()).unwrap() as u8);
        }
        ColumnTypeDict::MysqlTypeShort => {
            tmp = vec![0u8; 2];
        }
        ColumnTypeDict::MysqlTypeInt24 => {
            tmp = vec![0u8; 3];
        }
        ColumnTypeDict::MysqlTypeLong => {
            tmp = vec![0u8; 4];
        }
        ColumnTypeDict::MysqlTypeLonglong => {
            tmp = vec![0u8; 8];
        }
        ColumnTypeDict::MysqlTypeNewdecimal => {
            let decimal_meta = crate::replication::parsevalue::DecimalMeta::new(col_meta[0] as u8, col_meta[1] as u8);
            tmp = vec![0u8; decimal_meta.bytes_to_read];
        }
        ColumnTypeDict::MysqlTypeDouble |
        ColumnTypeDict::MysqlTypeFloat => {
            match col_meta[0] {
                8 => {
                    tmp = vec![0u8; 8];
                },
                4 => tmp = vec![0u8; 4],
                _ => {}
            }
        }
        ColumnTypeDict::MysqlTypeTimestamp2 => {
            let frac_part = read_datetime_fsp(col_meta[0] as u8);
            tmp = vec![0u8; (4 + frac_part) as usize];
        }
        ColumnTypeDict::MysqlTypeDatetime2 => {
            let subsecond = read_datetime_fsp(col_meta[0] as u8);
            tmp = vec![0u8; (5 + subsecond) as usize];
        }
        ColumnTypeDict::MysqlTypeYear => {
            tmp = vec![0u8; 1];
        }
        ColumnTypeDict::MysqlTypeDate => {
            tmp = vec![0u8; 3];

        }
        ColumnTypeDict::MysqlTypeTime2 => {
            let frac_part = read_datetime_fsp(col_meta[0] as u8);
            tmp = vec![0u8; (3 + frac_part) as usize];
        }
        ColumnTypeDict::MysqlTypeVarString |
        ColumnTypeDict::MysqlTypeVarchar |
        ColumnTypeDict::MysqlTypeBlob |
        ColumnTypeDict::MysqlTypeTinyBlob |
        ColumnTypeDict::MysqlTypeLongBlob |
        ColumnTypeDict::MysqlTypeMediumBlob |
        ColumnTypeDict::MysqlTypeBit => {
            let (var_bytes,var_length) =  read_str_value_length(buf, &col_meta[0]);
            tmp = vec![0u8; var_length];
            row_bytes.extend(var_bytes);

        }
        ColumnTypeDict::MysqlTypeJson => {
            let (var_bytes,var_length) =  read_str_value_length(buf, &col_meta[0]);
            tmp = vec![0u8; var_length];
            row_bytes.extend(var_bytes);
        }
        ColumnTypeDict::MysqlTypeString => {
            let mut value_length = 0;
            //println!("aa:{},{}",col_meta[0],buf.tell().unwrap());
            if col_meta[0] <= 255 {
                value_length = buf.read_u8().unwrap() as usize;
                row_bytes.push(value_length as u8);
            }
            else {
                let mut var_bytes = [0u8; 2];
                buf.read_exact(&mut var_bytes);
                row_bytes.extend(&var_bytes);
                buf.seek(SeekFrom::Current(-2));
                value_length = buf.read_u16::<LittleEndian>().unwrap() as usize;
            }
            tmp = vec![0u8; value_length];
        }
        ColumnTypeDict::MysqlTypeEnum |
        ColumnTypeDict::MysqlTypeSet => {
            match col_meta[0] {
                1 => {
                    tmp = vec![0u8; 1];
                },
                2 => {
                    tmp = vec![0u8; 2];
                }
                _ => {}
            }
        }
        _ => {}
    }
    if tmp.len() > 0 {
        buf.read_exact(tmp.as_mut());
    }
    row_bytes.extend(tmp);
    row_bytes
}

fn read_datetime_fsp(column: u8) -> u8 {
    match column {
        0 => 0,
        1 | 2 => 1,
        3 | 4 => 2,
        5 | 6 => 3,
        _ => 0,
    }
}

fn read_str_value_length<R: Read + Seek>(buf: &mut R, meta: &usize) -> (Vec<u8>,usize) {
    let mut var_bytes = vec![];
    let mut var_len: usize = 0;
    match meta {
        1 => {
            var_len = buf.read_u8().unwrap() as usize;
            var_bytes.push(var_len as u8);
        },
        2 => {
            var_bytes = vec![0u8; 2];
            buf.read_exact(var_bytes.as_mut());
            buf.seek(SeekFrom::Current(-2));
            var_len = buf.read_u16::<LittleEndian>().unwrap() as usize;
        },
        3 => {
            var_bytes = vec![0u8; 3];
            buf.read_exact(var_bytes.as_mut());
            buf.seek(SeekFrom::Current(-3));
            var_len = buf.read_u24::<LittleEndian>().unwrap() as usize;
        },
        4 => {
            var_bytes = vec![0u8; 4];
            buf.read_exact(var_bytes.as_mut());
            buf.seek(SeekFrom::Current(-4));
            var_len = buf.read_u32::<LittleEndian>().unwrap() as usize
        },
        5 => {
            var_bytes = vec![0u8; 5];
            buf.read_exact(var_bytes.as_mut());
            let tmp = var_bytes.clone();
            var_len = readvalue::read_u40(&tmp) as usize;
        }
        6 => {
            var_bytes = vec![0u8; 6];
            buf.read_exact(var_bytes.as_mut());
            let tmp = var_bytes.clone();
            var_len= readvalue::read_u48(&tmp) as usize;
        }
        7 => {
            var_bytes = vec![0u8; 7];
            buf.read_exact(var_bytes.as_mut());
            let tmp = var_bytes.clone();
            var_len = readvalue::read_u56(&tmp) as usize;
        }
        8 => {
            var_bytes = vec![0u8; 8];
            let tmp = var_bytes.clone();
            buf.read_exact(var_bytes.as_mut());
            var_len = readvalue::read_u64(&tmp) as usize;
        }
        _ => {}
    }
    (var_bytes, var_len)
}


