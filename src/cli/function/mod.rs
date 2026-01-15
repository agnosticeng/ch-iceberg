mod iceberg_create_table;
mod iceberg_field_bound_values;

use anyhow::Result;
use clap::{Args, Subcommand};
use iceberg_create_table::IcebergCreateTableCommand;
use iceberg_field_bound_values::IcebergFieldBoundValuesCommand;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommand {
    IcebergCreateTable(IcebergCreateTableCommand),
    IcebergFieldBoundValues(IcebergFieldBoundValuesCommand),
}

#[derive(Clone, Debug, Args)]
pub struct Function {
    #[command(subcommand)]
    pub cmd: FunctionCommand,
}

impl Function {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            FunctionCommand::IcebergCreateTable(cmd) => cmd.run().await,
            FunctionCommand::IcebergFieldBoundValues(cmd) => cmd.run().await,
        }
    }
}
