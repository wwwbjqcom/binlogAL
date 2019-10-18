
use std::process;
use mytest;
use structopt::StructOpt;

fn main() {
    let args = mytest::Opt::from_args();
    let config = mytest::Config::new(args).unwrap_or_else(|err|{
        println!("Problem parsing arguments: {}", err);
        process::exit(1);
    });
    println!("{:?}",config);
    mytest::startop(&config);



}
