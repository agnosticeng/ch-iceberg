mod iceberg_append;

use anyhow::Result;
use clap::{Args, Subcommand};
use iceberg_append::IcebergAppendCommand;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Subcommand)]
pub enum TableFunctionCommand {
    IcebergAppend(IcebergAppendCommand),
}

#[derive(Clone, Debug, Args)]
pub struct TableFunction {
    #[command(subcommand)]
    pub cmd: TableFunctionCommand,
}

impl TableFunction {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            TableFunctionCommand::IcebergAppend(cmd) => cmd.run().await,
        }
    }
}
