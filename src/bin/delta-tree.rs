extern crate deltalake;
extern crate anyhow;

use uuid::Uuid;
use std::env;
use deltatree::tree;

#[tokio::main(flavor="current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if let Some(table_path) = args.get(1) {
        println!("reading delta table: {:?}", table_path);
        let delta_table = deltalake::open_table(table_path).await?;
        build_tree(&delta_table);
        let uuid = Uuid::parse_str("477077ae-1429-4633-b07a-0c0cb75caf55").unwrap();
        println!("{:#x}", uuid.as_u128());
        Ok(())
    } else {
        println!("no file argument given.");
        Ok(())
    }
}

fn build_tree(delta_table: &deltalake::DeltaTable) -> tree::DeltaTree {
    tree::DeltaTree::new(delta_table)
}

fn estimate_tree_memory(_tree: tree::DeltaTree) -> usize {
    0
}