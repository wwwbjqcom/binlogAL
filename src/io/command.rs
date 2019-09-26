/*
@author: xiao cai niao
@datetime: 2019/9/24
*/

use std::net::TcpStream;
use crate::io::response;
use crate::io::socketio;
use crate::io::pack;
use std::process;
use crate::readvalue;
use std::collections::HashMap;

trait ColInit {
    fn new(buf: &Vec<u8>) -> Self;
}

//查询数据时mysql返回的字段元数据
#[derive(Debug)]
struct MetaColumn{
    catalog: String,
    schema: String,
    table: String,
    org_table: String,
    name: String,
    org_name: String,
    character_set: u16,
    column_length: u32,
    column_type: u8,
    flag: u16
}

impl ColInit for MetaColumn{
    fn new(buf: &Vec<u8>) -> MetaColumn {
        let mut offset: usize = 0;
        let mut var_size = buf[0] as usize ; //字段信息所占长度
        offset += 1;
        let catalog = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let schema = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let table = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let org_table = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let name = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let org_name = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        offset += 1;

        let character_set = readvalue::read_u16(&buf[offset..offset+2]);
        offset += 2;

        let column_length = readvalue::read_u32(&buf[offset..offset+4]);
        offset += 4;

        let column_type = buf[offset];
        offset +=1;

        let flag = readvalue::read_u16(&buf[offset..offset+2]);

        MetaColumn{
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            character_set,
            column_length,
            column_type,
            flag
        }
    }
}


pub fn execute(conn: &mut TcpStream,sql: &String) -> Vec<HashMap<String,String>>{
    let pack = commquery(sql);
    socketio::write_value(conn,&pack).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

    let a = unpack_text_packet(conn).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

    return a;
}

//组装COM_Query包
fn commquery(sql: &String) -> Vec<u8>{
    let mut pack = vec![];
    let mut payload = vec![];
    payload.push(3); //0x03: COM_QUERY
    payload.extend(sql.clone().into_bytes());
    let header = response::pack_header(&payload,0);
    pack.extend(header);
    pack.extend(payload);
    return pack;
}

fn unpack_text_packet(conn: &mut TcpStream) -> Result<Vec<HashMap<String,String>>,&'static str> {
    let (buf,_) = socketio::get_packet_from_stream(conn);
    if pack::check_pack(&buf){
        let mut values_info = vec![];   //数据值
        let mut column_info = vec![];   //每个column的信息

        let column_count = buf[0];
        for number in (0..column_count).rev(){
            let (buf,_) = socketio::get_packet_from_stream(conn);
            let column = MetaColumn::new(&buf);
            column_info.push(column);
        }

        //开始获取返回数据
        loop {
            let (mut buf,header) = socketio::get_packet_from_stream(conn);
            while header.payload == 0xffffff{
                println!("{}",header.payload);
                let (buf_tmp,header) = socketio::get_packet_from_stream(conn);
                buf.extend(buf_tmp);
            }
            if buf[0] == 0x00{
                break;
            }else if buf[0] == 0xfe {
                break;
            }
            let values = unpack_text_value(&buf, &column_info);
            values_info.push(values);
        }
        Ok(values_info)
    }else {
        let _err = readvalue::read_string_value(&buf[3..]);
        println!("执行语句错误: {}",_err);
        Err("退出程序")
    }
}

fn unpack_text_value(buf: &Vec<u8>,column_info: &Vec<MetaColumn>) -> HashMap<String,String> {
    //解析每行数据
    let mut values_info = HashMap::new();
    let mut offset = 0;
    for cl in column_info.iter(){
        let mut value = String::from("");
        let cl_name = cl.name.clone();
        let mut var_len = buf[offset] as usize;
        offset += 1;
        if var_len == 0xfc {
            var_len = readvalue::read_u16(&buf[offset..offset + 2]) as usize;
            offset += 2;
            value = readvalue::read_string_value(&buf[offset..offset + var_len]);
            offset += var_len;
        }
        else if var_len == 0xfd {
            var_len = readvalue::read_u24(&buf[offset..offset + 3]) as usize;
            offset += 3;
            value = readvalue::read_string_value(&buf[offset..offset + var_len]);
            offset += var_len;
        }
        else if var_len == 0xfe {
            var_len = readvalue::read_u64(&buf[offset..offset + 8]) as usize;
            offset += 8;
            value = readvalue::read_string_value(&buf[offset..offset + var_len]);
            offset += var_len;
        }
        else if var_len == 0xfb {
            value = String::from("");
        }
        else {
            value = readvalue::read_string_value(&buf[offset..offset + var_len]);
            offset += var_len;
        }


        values_info.insert(cl_name,value);
    }

    return values_info;
}



