/*
@author: xiao cai niao
@datetime: 2019/9/29
*/

use crate::meta::JsonType;
use crate::readvalue;
use std::io::Read;
use byteorder::{ReadBytesExt, LittleEndian};
use std::process;
use crate::Opt;
use std::collections::HashMap;
use serde_json::Value as JsonValue;
use serde_json::map::Map as JsonMap;
use failure::_core::iter::FromIterator;

pub fn read_binary_json<R: Read>(buf: &mut R, var_length: &usize) -> JsonValue {
    let t = buf.read_u8().unwrap() as usize;
    read_binary_json_type(buf, var_length, &t)
}

fn read_binary_json_type<R: Read>(buf: &mut R, var_length: &usize, m: &usize) -> JsonValue {
    let var_length = var_length.clone() - 1;
    let json_type_code = JsonType::from_type_code(m);
    println!("{:?}",json_type_code);
    let mut large = false;
    match json_type_code {
        JsonType::JSONB_TYPE_LARGE_OBJECT |
        JsonType::JSONB_TYPE_LARGE_ARRAY => {
            large = true;
        }
        _ => {
            large = false;
        }
    }

    match json_type_code {
        JsonType::JSONB_TYPE_LARGE_OBJECT |
        JsonType::JSONB_TYPE_SMALL_OBJECT => {
            JsonValue::from(read_binary_json_object(buf, &var_length, &large))
        }
        JsonType::JSONB_TYPE_LARGE_ARRAY |
        JsonType::JSONB_TYPE_SMALL_ARRAY => {
            JsonValue::from(read_binary_json_array(buf, &var_length, &large))
        }
        JsonType::JSONB_TYPE_STRING => {
            let mut byte = 0x80 as usize;
            let mut length = 0 as usize;
            let mut bits_read = 0 as usize;
            while byte & 0x80 != 0{
                byte = buf.read_u8().unwrap() as usize;
                length = length | ((byte & 0x7f) << bits_read);
                bits_read = bits_read + 7;
            }
            JsonValue::from(readvalue::read_string_value_from_len(buf, length))
        }
        JsonType::JSONB_TYPE_LITERAL => {
            let value = buf.read_u8().unwrap() as usize;
            let type_code = JsonType::from_type_code(&value);
            match type_code {
                JsonType::JSONB_LITERAL_NULL => JsonValue::Null,
                JsonType::JSONB_LITERAL_TRUE => JsonValue::from(true),
                JsonType::JSONB_LITERAL_FALSE => JsonValue::from(false),
                _ => {JsonValue::Null}
            }
        }
        JsonType::JSONB_TYPE_INT16 => {
            JsonValue::from(buf.read_i16::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_UINT16 => {
            JsonValue::from(buf.read_u16::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_DOUBLE => {
            JsonValue::from(buf.read_f64::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_INT32 => {
            JsonValue::from(buf.read_i32::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_UINT32 => {
            JsonValue::from(buf.read_u32::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_INT64 =>{
            JsonValue::from(buf.read_i64::<LittleEndian>().unwrap())
        }
        JsonType::JSONB_TYPE_UINT64 => {
            JsonValue::from(buf.read_u64::<LittleEndian>().unwrap())
        }
        _ => {
            println!("无效的json格式:{:?}",json_type_code);
            process::exit(1)
        }
    }
}

fn read_binary_json_object<R: Read>(buf: &mut R, var_length: &usize, large: &bool) -> JsonValue {
    let mut elements: usize;
    let mut size: usize;
    let mut key_offset_lengths: Vec<Vec<usize>> = vec![];

    if *large {
        elements = buf.read_u32::<LittleEndian>().unwrap() as usize;
        size = buf.read_u32::<LittleEndian>().unwrap() as usize;
    } else {
        elements = buf.read_u16::<LittleEndian>().unwrap() as usize;
        size = buf.read_u16::<LittleEndian>().unwrap() as usize;
    }

    if size > *var_length {
        println!("json长度大于包长度, 现在退出程序！！！");
        process::exit(1);
    }

    let mut key_offset_lengths = vec![];
    if *large{
        for i in (0..elements){
            let mut tmp_dict: Vec<usize> = vec![];
            tmp_dict.push(buf.read_u32::<LittleEndian>().unwrap() as usize);
            tmp_dict.push(buf.read_u16::<LittleEndian>().unwrap() as usize);
            key_offset_lengths.push(tmp_dict);

        }
    } else {
        for i in (0..elements){
            let mut tmp_dict: Vec<usize> = vec![];
            tmp_dict.push(buf.read_u16::<LittleEndian>().unwrap() as usize);
            tmp_dict.push(buf.read_u16::<LittleEndian>().unwrap() as usize);
            key_offset_lengths.push(tmp_dict);
        }
    }

    let mut value_type_inlined_lengths: Vec<ValuesTypeInline> = vec![];
    for i in (0..elements) {
        let tmp = ValuesTypeInline::new(buf, large);
        value_type_inlined_lengths.push(tmp);
    }

    let mut keys: Vec<String> = vec![];
    for v in key_offset_lengths.iter(){
        keys.push(readvalue::read_string_value_from_len(buf, v[1]));
    }

    let mut values = Vec::with_capacity(elements);
    for i in (0..elements){
        let value_inlined = &value_type_inlined_lengths[i];
        match value_inlined.b {
            None => {
                let data = &value_inlined.c;
                match data.a {
                    None => {
                        if data.b == 0 {
                            values.push(JsonValue::from(false));
                        }else if data.b == 1 {
                            values.push(JsonValue::from(true));
                        }else {
                            values.push(JsonValue::Null);
                        }
                    }
                    Some(v) => {
                        values.push(JsonValue::from(v));
                    }
                    _ => {}
                }
            }
            _ => {
                let t = value_inlined.a;
                let data = read_binary_json_type(buf,var_length,&t);
                values.push(JsonValue::from(data));
            }
        }
    }

    if keys.len() > 0 {
        let map = JsonMap::from_iter(keys.into_iter().zip(values.into_iter()));
        JsonValue::Object(map)
    }else {
        JsonValue::Array(values)
    }



}
#[derive(Debug)]
struct LiteralInline {
    a: Option<usize>, // default： None
    b: i8, // 0: false, 1: true, -1: default
}
impl LiteralInline{
    fn new<R: Read>(buf: &mut R, type_code: &JsonType) -> LiteralInline{
        let mut a = None;
        let mut b = -1;
        match type_code {
            JsonType::JSONB_TYPE_LITERAL => {
                let value = buf.read_u16::<LittleEndian>().unwrap() as usize;
                let code = JsonType::from_type_code(&value);
                match code {
                    JsonType::JSONB_LITERAL_NULL => {
                        a = None;
                    },
                    JsonType::JSONB_LITERAL_TRUE => {
                        b = 1;
                    },
                    JsonType::JSONB_LITERAL_FALSE => {
                        b = 0;
                    },
                    _ => {}
                }
            }
            JsonType::JSONB_TYPE_INT16 => {
                let tmp = buf.read_i16::<LittleEndian>().unwrap() as usize;
                a = Some(tmp);
            },
            JsonType::JSONB_TYPE_UINT16 => {
                let tmp = buf.read_u16::<LittleEndian>().unwrap() as usize;
                a = Some(tmp);
            },
            JsonType::JSONB_TYPE_INT32 => {
                let tmp = buf.read_i32::<LittleEndian>().unwrap() as usize;
                a = Some(tmp);
            },
            JsonType::JSONB_TYPE_UINT32 => {
                let tmp = buf.read_u32::<LittleEndian>().unwrap() as usize;
                a = Some(tmp);
            }
            _ => {}
        }

        LiteralInline{
            a,
            b,
        }
    }

    fn default() -> LiteralInline {
        LiteralInline{
            a: None,
            b: -1
        }
    }

}
#[derive(Debug)]
struct ValuesTypeInline {
    a: usize,
    b: Option<usize>,
    c: LiteralInline,
}
impl ValuesTypeInline{
    fn new<R: Read>(buf: &mut R, large: &bool) -> ValuesTypeInline {
        let a = buf.read_u8().unwrap() as usize;
        let mut b = None;
        let mut c = LiteralInline::default();
        let json_type_code = JsonType::from_type_code(&a);
        match json_type_code {
            JsonType::JSONB_TYPE_LITERAL |
            JsonType::JSONB_TYPE_INT16 |
            JsonType::JSONB_TYPE_UINT16 => {
                c = LiteralInline::new(buf,&json_type_code);
                return ValuesTypeInline{a, b, c};
            }
            JsonType::JSONB_TYPE_INT32 |
            JsonType::JSONB_TYPE_UINT32 => {
                if *large{
                    c = LiteralInline::new(buf,&json_type_code);
                    return ValuesTypeInline{a, b, c};
                }
            }
            _ => {}
        }
        if *large{
            b = Some(buf.read_u32::<LittleEndian>().unwrap() as usize);
            ValuesTypeInline{a, b, c}
        }
        else {
            b = Some(buf.read_u16::<LittleEndian>().unwrap() as usize);
            ValuesTypeInline{a, b, c}
        }

    }

}

fn read_binary_json_array<R: Read>(buf: &mut R, var_length: &usize, large: &bool) -> JsonValue {
    let elements: usize;
    let size: usize;
    if *large{
        elements = buf.read_u32::<LittleEndian>().unwrap() as usize;
        size = buf.read_u32::<LittleEndian>().unwrap() as usize;
    }
    else {
        elements = buf.read_u16::<LittleEndian>().unwrap() as usize;
        size = buf.read_u16::<LittleEndian>().unwrap() as usize;
    }
    if size > *var_length{
        println!("json长度大于包长度, 现在退出程序！！！");
        process::exit(1);
    }

    let mut value_type_inlined_lengths: Vec<ValuesTypeInline> = vec![];
    for i in (0..elements) {
        let tmp = ValuesTypeInline::new(buf, large);
        value_type_inlined_lengths.push(tmp);
    }

    let mut values = Vec::with_capacity(elements);
    for i in (0..elements) {
        let value_inlined = &value_type_inlined_lengths[i];
        match value_inlined.b {
            None => {
                let data = &value_inlined.c;
                match data.a {
                    None => {
                        if data.b == 0 {
                            values.push(JsonValue::from(false));
                        }else if data.b == 1 {
                            values.push(JsonValue::from(true));
                        }else {
                            values.push(JsonValue::Null);
                        }
                    }
                    Some(v) => {
                        values.push(JsonValue::from(v));
                    }
                    _ => {}
                }
            }
            _ => {
                let t = value_inlined.a;
                let data = read_binary_json_type(buf,var_length,&t);
                values.push(JsonValue::from(data));
            }
        }
    }

    JsonValue::Array(values)
}
