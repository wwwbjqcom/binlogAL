/*
@author: xiao cai niao
@datetime: 2019/9/21
*/
use crate::readvalue;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::error::Error;

//包头部分
#[derive(Debug)]
pub struct PacketHeader {
    pub payload: u32,
    pub seq_id: u8,
}

impl PacketHeader{
    pub fn new(buf: &[u8]) -> PacketHeader{
        let payload = readvalue::read_u24(&buf[..3]);
        PacketHeader{
                payload,
                seq_id: buf[3].clone()
            }

    }
}

fn get_from_stream(stream: &mut TcpStream) -> (Vec<u8>, PacketHeader){
    //获取一个数据包
    //定义4个u8的vector接收包头4bytes数据

    let mut header_buf = vec![0 as u8; 4];
    let mut header: PacketHeader = PacketHeader { payload: 0, seq_id: 0 };
    loop {
        match stream.read_exact(&mut header_buf){
            Ok(_) => {
                header = PacketHeader::new(&header_buf);
                if header.payload > 0 {
                    break;
                }
            }
            Err(_) => {
                //println!("{}",e);
            }
        }

    }

    //通过包头获取到的payload数据读取实际数据
    let mut packet_buf  = vec![0 as u8; header.payload as usize];
    match stream.read_exact(&mut packet_buf) {
        Ok(_) =>{}
        Err(_) => {
            println!("read packet error");
        }
    }

    return (packet_buf,header);
}

pub fn get_packet_from_stream(stream: &mut TcpStream) -> (Vec<u8>, PacketHeader){
    let (mut buf,header) = get_from_stream(stream);
    while header.payload == 0xffffff{
        println!("{}",header.payload);
        let (buf_tmp,header) = get_from_stream(stream);
        buf.extend(buf_tmp);
    }
    (buf, header)
}


//向连接写入数据
pub fn write_value(stream: &mut TcpStream, buf: &Vec<u8>) -> Result<(),Box<dyn Error>> {
    stream.write_all(buf)?;
    Ok(())
}

