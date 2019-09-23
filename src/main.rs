
use std::process;
use mytest;
use structopt::StructOpt;


fn main() {
    let args = mytest::Opt::from_args();
    //println!("{:?}", opt);

    //let args: Vec<String> = env::args().collect();
    let config = mytest::Config::new(args).unwrap_or_else(|err|{
        println!("Problem parsing arguments: {}", err);
        process::exit(1);
    });
    println!("{:?}",config);
    mytest::startop(&config);



//    let mut tcp_conn = mytest::conn(&config.host_info).unwrap_or_else(|err|{
//        println!("tcp_stream connection is error: {}",err);
//        process::exit(1);
//    });
//    let mut header_buf = [0 as u8; 4];
//    match  tcp_conn.read_exact(&mut header_buf){
//        Ok(_) => {}
//        Err(_) => {
//            println!("read packet error");
//        }
//    }
//
//    let header = mytest::PacketHeader::new(&header_buf).unwrap_or_else(|err|{
//        println!("{}",err);
//        process::exit(1);
//    });
//    //println!("{:?}",header);
//
//    let mut handshake_buf = vec![0 as u8; header.payload as usize];
//    match tcp_conn.read_exact(&mut handshake_buf) {
//        Ok(_) =>{}
//        Err(_) => {
//            println!("read packet error");
//        }
//    }
//    let handshake = mytest::HandshakePacket::new(&handshake_buf).unwrap_or_else(|err|{
//        println!("{}",err);
//        process::exit(1);
//    });
//    println!("{:?}",handshake);
//
//
//    let handshake_response = mytest::response::LocalInfo::new(args[0].borrow(), 0);
//    println!("{:?}",handshake_response);
//    let packet_type = mytest::meta::PackType::HandShakeResponse;
//    let v = mytest::response::LocalInfo::pack_payload(
//        &handshake_response,&handshake,&packet_type,&config).unwrap_or_else(
//        |err|{
//        println!("{}",err);
//        process::exit(1);
//    });
//    println!("{:?}",v);
//    tcp_conn.write_all(v.as_ref());
//
//    let mut header_buf = [0 as u8; 4];
//    match  tcp_conn.read_exact(&mut header_buf){
//        Ok(_) => {}
//        Err(_) => {
//            println!("read packet error");
//        }
//    }
//    println!("{:?}",header_buf);
//    let header = mytest::PacketHeader::new(&header_buf).unwrap_or_else(|err|{
//        println!("{}",err);
//        process::exit(1);
//    });
//
//    println!("{:?}",header);
//
//    let mut value = vec![0 as u8; header.payload as usize];
//    match  tcp_conn.read_exact(&mut value){
//        Ok(_) => {}
//        Err(_) => {
//            println!("read packet error");
//        }
//    }
//
//    println!("{},{}",value[0],mytest::readvalue::read_u16(&value[1..3]));
//    println!("{:?}",from_utf8(&value[3..].as_ref()));


//    let handshake_reponse = ::LocalInfo::new(args[0],0);
//    println!("{:?}",handshake_reponse);


}
