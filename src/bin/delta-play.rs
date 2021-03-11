extern crate anyhow;
extern crate deltalake;

use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if let Some(table_path) = args.get(1) {
        println!("reading delta table: {:?}", table_path);
        let delta_table = deltalake::open_table(table_path).await?;
        read_some_data(&delta_table);
        Ok(())
    } else {
        println!("no file argument given.");
        Ok(())
    }
}

fn read_some_data(delta_table: &deltalake::DeltaTable) -> () {
    let num_files = delta_table.get_files().len();
    let files = delta_table.get_files().iter().take(3);
    println!("delta has #{} parquet files", num_files);
    println!("schema: {:?}", delta_table.schema());
    println!(
        "characters: {:5}",
        estimate_file_name_memory_consumption(delta_table)
    );
    println!(
        "characters: {:5}",
        estimate_file_name_memory_consumption(delta_table)
    );
    files.for_each(|f| println!("f: {}", f));
    // let mut buf = String::new();
    // std::io::stdin().read_line(&mut buf);
    println!("thanks.");
}

fn estimate_file_name_memory_consumption(delta_table: &deltalake::DeltaTable) -> usize {
    delta_table
        .get_files()
        .iter()
        .map(|f| f.len())
        .fold(0, |a, b| a + b)
}
