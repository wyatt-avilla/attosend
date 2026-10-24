use clap::Parser;

#[derive(Parser)]
#[command(version, about)]
struct Args {}

fn main() {
    Args::parse();
    println!("Hello, world!");
}
