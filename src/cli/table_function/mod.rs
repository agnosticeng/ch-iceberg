mod iceberg_append_static_table;

use anyhow::Result;
use clap::{Args, Subcommand};
use iceberg_append_static_table::IcebergAppendStaticTableCommand;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Subcommand)]
pub enum TableFunctionCommand {
    IcebergAppendStaticTable(IcebergAppendStaticTableCommand),
}

#[derive(Clone, Debug, Args)]
pub struct TableFunction {
    #[command(subcommand)]
    pub cmd: TableFunctionCommand,
}

impl TableFunction {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            TableFunctionCommand::IcebergAppendStaticTable(cmd) => cmd.run().await,
        }
    }
}
