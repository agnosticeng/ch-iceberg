mod iceberg_create_static_table;
mod iceberg_field_bound_values_static_table;

use anyhow::Result;
use clap::{Args, Subcommand};
use iceberg_create_static_table::IcebergCreateStaticTableCommand;
use iceberg_field_bound_values_static_table::IcebergFieldBoundValuesStaticTableCommand;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommand {
    IcebergCreateStaticTable(IcebergCreateStaticTableCommand),
    IcebergFieldBoundValuesStaticTable(IcebergFieldBoundValuesStaticTableCommand),
}

#[derive(Clone, Debug, Args)]
pub struct Function {
    #[command(subcommand)]
    pub cmd: FunctionCommand,
}

impl Function {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            FunctionCommand::IcebergCreateStaticTable(cmd) => cmd.run().await,
            FunctionCommand::IcebergFieldBoundValuesStaticTable(cmd) => cmd.run().await,
        }
    }
}
