use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

pub mod cli;
pub mod iceberg;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    cli::CLI::parse().run()
}
