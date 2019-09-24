/*
@author: xiao cai niao
@datetime: 2019/9/19
*/

use crate::{readvalue, meta};
use crate::io::pack::HandshakePacket;
use crate::meta::PackType;
use std::convert::TryInto;
use std::process;
use crate::Config;
use crate::io::scramble;
//
//pub trait pack{
//    fn pack_header(buf: &[u8], seq: u8)-> Vec<u8>;
//    fn pack_payload<T>(buf: &T,pack_type: &PackType,config: &Config) -> Vec<u8>;
//}

//handshakereponse回包基础信息
#[derive(Debug)]
pub struct LocalInfo{
    pub client_name: String,
    pub pid: u32,
    pub client_version: String,
    pub program_name: String,
    pub client_flag: i32,
    pub max_packet_size : u32
}

impl LocalInfo {
    pub fn new(program: &String,database: u8) -> Self{
        let mut client_flag = 0;
        let flags_meta = meta::FlagsMeta::new();
        let capabilities = flags_meta.long_password|
            flags_meta.long_flag|flags_meta.protocol_41|flags_meta.transactions|
            flags_meta.secure_connection|flags_meta.multi_results|
            flags_meta.client_plugin_auth| flags_meta.client_plugin_auth_lenenc_client_data|
            flags_meta.client_connect_attrs|flags_meta.client_deprecate_eof;
        client_flag |= capabilities;
        if database > 0{
            client_flag |= flags_meta.client_connect_with_db
        }
        client_flag |= flags_meta.multi_results;
        Self{
            client_name: String::from("rust_client"),
            pid:123,
            client_version:String::from("8.0.17"),
            program_name: program.clone(),
            client_flag,
            max_packet_size:16777215
        }
    }

    pub unsafe fn pack_handshake(&self, buf: &HandshakePacket, config: &Config, database: u8) -> Vec<u8>{
        //组包
        let mut rdr = Vec::new();
        let flags_meta = meta::FlagsMeta::new();
        //rdr.extend(buf.server_version.clone().into_bytes());
        rdr.extend(readvalue::write_i32(self.client_flag));
        rdr.extend(readvalue::write_u32(self.max_packet_size));
        rdr.push(buf.character_set_id);
        let mut num = 0;
        while num < 23{
            //rdr.extend(String::from("").into_bytes());
            rdr.push(0);
            num += 1;
        }

        rdr.extend(config.user_name.clone().into_bytes());
        rdr.push(0);

        let sha1_pass;
        match scramble::scramble_native(buf.auth_plugin_data[..20].as_ref(), config.password.as_bytes()){
            Some(value) => {sha1_pass=value},
            None => process::exit(1)

        };
        if buf.capability_flags & (flags_meta.client_plugin_auth_lenenc_client_data as u32) > 0{
            rdr.push(sha1_pass.len() as u8);
            rdr.extend(sha1_pass.iter());
        }else if buf.capability_flags & (flags_meta.secure_connection as u32) > 0 {
                rdr.push(sha1_pass.len() as u8);
                rdr.extend(sha1_pass.iter());
            }else {
            rdr.extend(sha1_pass.iter());
            rdr.push(0);
        }

        if buf.capability_flags & (flags_meta.client_connect_with_db as u32) > 0{
            if database > 0{
                rdr.extend(config.database.clone().into_bytes())
            }
            rdr.push(0);
        }
        if buf.capability_flags & flags_meta.client_plugin_auth as u32 > 0{
            //rdr.extend(String::from("").into_bytes());
            rdr.push(0);
            rdr.push(0);
        }

        let connect_attrs = Self::pack_connect_attrs(self);
        if buf.capability_flags & (flags_meta.client_connect_attrs as u32) > 0{
            rdr.push(connect_attrs.len().try_into().unwrap());
            rdr.extend(connect_attrs);
        }

        return rdr;
    }

    fn pack_connect_attrs(&self) -> Vec<u8> {
        let mut connect_attrs = Vec::new();
        connect_attrs.push(self.client_name.len() as u8);
        connect_attrs.extend(self.client_name.clone().into_bytes());
        connect_attrs.push(self.client_version.len() as u8);
        connect_attrs.extend(self.client_version.clone().into_bytes());
        connect_attrs.push(self.program_name.len() as u8);
        connect_attrs.extend(self.program_name.clone().into_bytes());

        return connect_attrs;
    }


}

impl LocalInfo {

    pub fn pack_payload(&self, buf: &HandshakePacket, pack_type: &PackType, config: &Config) -> Result<Vec<u8> ,&'static str> {
        //组装handshakeresponse包从这里开始，返回一个完整的回包
        //所有数据都放于verctor类型中
        let mut _payload = Vec::new();
        let mut payload_value: Vec<u8> = vec![];
        let mut packet_header: Vec<u8> = vec![];
        match pack_type {
            PackType::HandShake => {},
            PackType::HandShakeResponse => unsafe {
                if config.database.len() > 0 {
                    payload_value = Self::pack_handshake(&self,buf, config, 1);
                }else {
                    payload_value = Self::pack_handshake(&self,buf, config, 0) ;
                }
            }
            _ => payload_value = vec![]
        }

        if payload_value.len() > 0{
            packet_header = pack_header(&payload_value,1)
        }else {
            return Err("paket payload is failed!!");
        }
        _payload.extend(packet_header);
        _payload.extend(payload_value);
        return Ok(_payload);
    }
}

//auth_switch需再次验证密码方式
pub fn authswitchrequest(handshake: &HandshakePacket,buf: &Vec<u8>,conf: &Config) -> Vec<u8> {
    let mut packet: Vec<u8> = vec![];
    let mut payload: Vec<u8> = vec![];
    let mut offset = 1;
    let mut auth_plugin_name = String::from("");
    for (b,item )in buf[offset..].iter().enumerate(){
        if item == &0x00 {
            auth_plugin_name = readvalue::read_string_value(&buf[offset..offset+b]);
            offset += b + 1;
            break;
        }
    }
    let auth_plugin_data = &buf[offset..];

    if auth_plugin_name.len() > 0 {
        let flags_meta = meta::FlagsMeta::new();
        if handshake.capability_flags & flags_meta.client_plugin_auth as u32 > 0 {
            match scramble::scramble_native(&auth_plugin_data[..20], conf.password.as_bytes()){
                Some(value) => {payload= value.to_vec() },
                None => process::exit(1)
            }
        }
    }

    packet.extend(pack_header(payload.as_ref(), 3));
    packet.extend(payload);
    return packet;
}

//组装heder部分
pub fn pack_header(buf: &[u8], seq: u8) -> Vec<u8> {
    let mut _header = Vec::new();
    let payload = readvalue::write_u24(buf.len() as u32);
    _header.extend(payload);
    _header.push(seq);
    return _header;
}