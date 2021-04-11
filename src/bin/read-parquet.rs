extern crate parquet;

use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{RowAccessor, Row};
use parquet::record::reader::RowIter;

fn main() {
    let args: Vec<String> = env::args().collect();
    args.iter().skip(1).enumerate().for_each(|(idx, arg)| {
        let num_records = read_file(arg);
        println!("{}: {:#?} ({} records)", idx, arg, num_records);
    })
}

const DESIRED_INDEX: i64 = 5000000;

fn read_file(path: &str) -> usize {
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut rows = 0;
    for group_index in 0..reader.num_row_groups() {
        let row_group = reader.get_row_group(group_index).unwrap();
        let metadata = row_group.metadata();
        rows += metadata.num_rows();
        if let Some(stats) = metadata.column(0).statistics() {
            let min = read_le_i64(stats.min_bytes());
            let max = read_le_i64(stats.max_bytes());
            if DESIRED_INDEX >= min && DESIRED_INDEX <= max {
                println!("scanning: min: '{:#?}', max: '{:#?}'", min, max);
                scan_rows(row_group.get_row_iter(None).unwrap());
            } else {
                println!("skipping: min: '{:#?}', max: '{:#?}'", min, max);
            }
        } else {
            scan_rows(row_group.get_row_iter(None).unwrap());
        }
        // println!("meta data: {:#?}", metadata);
    }
    // scan_rows(reader.get_row_iter(None).unwrap());
    rows as usize
}

fn scan_rows(mut iter: RowIter) -> Option<Row> {
    while let Some(record) = iter.next() {
        let idx = record.get_long(0).unwrap();
        if idx == 5000000 {
            println!("{}", record);
            return Some(record);
        }
    }
    None
}

fn read_le_i64(input: &[u8]) -> i64 {
    i64::from_le_bytes(input.try_into().unwrap())
}