use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::DeltaTableBuilder;
use std::env;
use std::future::IntoFuture;
use std::collections::HashMap;

use deltalake::{DeltaTableError, PartitionFilter};
use tracing_subscriber::FmtSubscriber;

#[allow(dead_code)]
enum PartitionFilterValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
}

fn convert_partition_filters<'a>(
    partitions_filters: Vec<(&'a str, &'a str, PartitionFilterValue)>,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => PartitionFilter::try_from((key, op, v)),
            (key, op, PartitionFilterValue::Multiple(v)) => {
                PartitionFilter::try_from((key, op, v.as_slice()))
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    // Set the environment variable for logging
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
        // Set up tracing subscriber for your application and its dependencies
        let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG) // Set maximum log level
        .finish();

    // Initialize global tracing subscriber
    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set tracing subscriber");

    deltalake::azure::register_handlers(None);
    let account = env::var("AZURE_STORAGE_ACCOUNT_NAME").expect("no account name set");
    let container = env::var("AZURE_CONTAINER_NAME").expect("no container name set");
    let key = env::var("AZURE_STORAGE_SAS_KEY").expect("no account key set");

    // Define the blob path and storage options
    let mut storage_options = HashMap::new();
    storage_options.insert("azure_storage_account_name".to_owned(), account);
    storage_options.insert("azure_container_name".to_owned(), container);
    storage_options.insert("azure_storage_sas_key".to_owned(), key);

    storage_options.insert("timeout".to_owned(), "120s".to_owned());
    let uri = "az://long-term-save/long-term-save/test-delta/AXS_fleet".to_string();
    
    let delta = DeltaTableBuilder::from_uri(uri.clone())
    .with_storage_options(storage_options);

    // Create a DeltaTable instance
    let mut dt = delta.load().await.unwrap();
    let z_order_columns = vec!["StatusDateTime".to_owned()];

    let partition_filters = None;
    let target_size = None;
    let max_concurrent_tasks = None;
    let max_spill_size = 20 * 1024 * 1024 * 1024;
    let min_commit_interval = None;

    let mut cmd = OptimizeBuilder::new(
        dt.log_store(),
        dt.snapshot()?.clone(),
    )
    .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
    .with_max_spill_size(max_spill_size)
    .with_type(OptimizeType::ZOrder(z_order_columns));
    if let Some(size) = target_size {
        cmd = cmd.with_target_size(size);
    }
    if let Some(commit_interval) = min_commit_interval {
        cmd = cmd.with_min_commit_interval(std::time::Duration::from_secs(commit_interval));
    }

    let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())?;
    cmd = cmd.with_filters(&converted_filters);

    let (table, metrics) = cmd.into_future().await?;
    dt.state = table.state;
    let _res = serde_json::to_string(&metrics).unwrap();
    Ok(())

}
