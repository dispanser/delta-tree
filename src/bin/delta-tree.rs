extern crate anyhow;
extern crate deltalake;

use deltatree::tree;
use deltatree::tree::DeltaTree;
use deltatree::tree::TreeNode;
use std::collections::hash_map::Entry;
use std::env;
use std::time::Instant;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if let Some(table_path) = args.get(1) {
        println!("reading delta table: {:?}", table_path);
        let start_load = Instant::now();
        let delta_table = deltalake::open_table(table_path).await?;
        let file_memory = estimate_file_memory(&delta_table);
        println!(
            "delta file memory: {} (time: {:?})",
            file_memory,
            start_load.elapsed()
        );
        let start_tree = Instant::now();
        let delta_tree = DeltaTree::new(&delta_table);
        let tree_memory = estimate_tree_memory(&delta_tree.root);
        println!(
            "delta tree memory: {} (time: {:?})",
            tree_memory,
            start_tree.elapsed()
        );
        println!("relative tree size: {} %", 100 * tree_memory / file_memory);
        Ok(())
    } else {
        println!("no file argument given.");
        Ok(())
    }
}

fn estimate_tree_memory(tree: &TreeNode) -> usize {
    match tree {
        TreeNode::FileEntries { files } => {
            std::mem::size_of::<tree::ParquetDeltaFile>() * files.capacity()
        }
        TreeNode::Partition { name, values } => values.iter().fold(
            std::mem::size_of::<Entry<String, TreeNode>>() + name.capacity(),
            |agg, (key, value)| agg + key.capacity() + estimate_tree_memory(value),
        ),
    }
}

fn estimate_file_memory(delta_table: &deltalake::DeltaTable) -> usize {
    delta_table
        .get_files()
        .iter()
        .map(|f| f.capacity())
        .fold(0, |a, b| a + b)
}
