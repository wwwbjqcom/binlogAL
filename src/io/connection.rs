/*
@author: xiao cai niao
@datetime: 2019/9/21
*/

use crate::{Config};
use std::net::TcpStream;
use std::process;
use crate::meta;
use crate::io::socketio;
use crate::io::response;
use std::time::Duration;
use std::error::Error;
use std::borrow::Borrow;
use std::str::from_utf8;

fn conn(host_info: &str) -> Result<TcpStream, Box<dyn Error>> {
    let tcp_conn = TcpStream::connect(host_info)?;
    tcp_conn.set_read_timeout(None)?;
    tcp_conn.set_write_timeout(Some(Duration::new(10,10)))?;
    Ok(tcp_conn)
}

pub fn create_mysql_conn(conf: &Config) -> Result<TcpStream, &'static str>{
    /*
    这里是与mysql建立连接的整个过程
    */
    let mut mysql_conn = conn(&conf.host_info).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

    let (packet_buf,header) = socketio::get_packet_from_stream(&mut mysql_conn);
    let handshake = socketio::HandshakePacket::new(&packet_buf).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

    //根据服务端发送的hand_shake包组回报并发送
    let mut tmp: u8 = 0;
    if conf.database > "0".to_string() {
        tmp = 1;
    }
    let handshake_response = response::LocalInfo::new(conf.program_name.borrow(), tmp);
    let packet_type = meta::PackType::HandShakeResponse;
    let v = response::LocalInfo::pack_payload(
        &handshake_response,&handshake,&packet_type,conf).unwrap_or_else(
        |err|{
            println!("{}",err);
            process::exit(1);
        });

    socketio::write_value(&mut mysql_conn, v.as_ref()).unwrap_or_else(|err|{
        println!("{}",err);
        process::exit(1);
    });

    //检查服务端回包情况
    let (packet_buf,header) = socketio::get_packet_from_stream(&mut mysql_conn);
    if packet_buf[0] == 0xFE {
        //重新验证
        let auth_data = response::authswitchrequest(&handshake, packet_buf.as_ref(), conf);
        socketio::write_value(&mut mysql_conn, auth_data.as_ref()).unwrap_or_else(|err|{
            println!("{}",err);
            process::exit(1);
        });
    }

    let (packet_buf,header) = socketio::get_packet_from_stream(&mut mysql_conn);

    //连接成功停留100秒
    use std::{thread, time};
    let ten_millis = time::Duration::from_secs(100);
    thread::sleep(ten_millis);


    Ok(mysql_conn)
}