mod function;
mod table_function;

use crate::cli::function::Function;
use crate::cli::table_function::TableFunction;
use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::runtime::Builder;

#[derive(Debug, Subcommand)]
pub enum Command {
    Function(Function),
    TableFunction(TableFunction),
}

#[derive(Parser)]
pub struct CLI {
    #[command(subcommand)]
    pub cmd: Command,
}

impl CLI {
    pub fn run(&self) -> Result<()> {
        Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()?
            .block_on(async {
                match &self.cmd {
                    Command::Function(cmd) => cmd.run().await,
                    Command::TableFunction(cmd) => cmd.run().await,
                }
            })
    }
}
