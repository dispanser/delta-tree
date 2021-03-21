extern crate anyhow;
extern crate deltalake;

use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if let Some(table_path) = args.get(1) {
        println!("reading delta table: {:?}", table_path);
        let mut delta_table = deltalake::open_table(table_path).await?;
        let mut buf = String::new();
        println!("{:?}", delta_table.get_files());
        println!("initial table loaded, version {:?}", delta_table.version);
        assert_eq!(delta_table.version as usize, delta_table.get_files().len() - 1);
        loop {
            println!("waiting on keypress for update, current version {:?}", delta_table.version);
            std::io::stdin().read_line(&mut buf);
            println!("updating ... ");
            delta_table.update().await?;
            println!("{:?}", delta_table.get_files());
            assert_eq!(delta_table.version as usize, delta_table.get_files().len() - 1);
        }
        // Ok(())

    } else {
        println!("no file argument given.");
        Ok(())
    }
}

