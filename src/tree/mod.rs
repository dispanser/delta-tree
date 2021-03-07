use deltalake;
use uuid::Uuid;
use regex::Regex;
use lazy_static::lazy_static;
use std::collections::HashMap;
use itertools::Itertools;

#[derive(Debug, PartialEq, Eq)]
pub struct DeltaTree {
    root: TreeNode,
}

#[derive(Debug, PartialEq, Eq)]
enum TreeNode {
    /// a partition is a key and a map of all its values to the next lower level in the tree.
    Partition {
        name:     String,                   // the key / column name of the partition
        values:   HashMap<String, TreeNode>, // partition values mapped to the content
    },

    /// represent the contents of a single leaf directory: a set of parquet files.
    FileEntries {
        files: Vec<ParquetDeltaFile>
    }
}

/// a single parquet file, represented in a compact partion / uuid / compression triple.
/// TODO: figure out if other name components are variable, e.g. `c000`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct ParquetDeltaFile {
    partition:        u32,
    uuid:             u128,
    compression: CompressionType,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionPath<'a> {
    key:   &'a str,
    value: &'a str,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum CompressionType {
    SNAPPY,
    GZIP,
    NONE,
}

impl CompressionType {
    fn from_str(s: &str) -> CompressionType {
        match s {
            "snappy" => CompressionType::SNAPPY,
            "gzip"   => CompressionType::GZIP,
            "none"   => CompressionType::NONE,
            _        => panic!("unexpected compression name, {}", s)
        }
    }
}

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new("^part-(?P<part>\\d{5})-\
                (?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-\
                [0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.c000\\.\
                (?P<compression>(snappy|gzip|none)).parquet").unwrap();
}

impl DeltaTree {

    pub fn new(delta_table: &deltalake::DeltaTable) -> DeltaTree {
        DeltaTree::from_paths(delta_table.get_files())
    }

    pub fn from_paths(input_files: &Vec<String>) -> DeltaTree {
        if input_files.is_empty() {
            DeltaTree {
                root: TreeNode::FileEntries { files: vec![] }
            }
        } else {
            let components: Vec<(Vec<PartitionPath>, ParquetDeltaFile)> = input_files.iter()
                .map(|f| f.split('/').collect())
                .map(|path| DeltaTree::parse_path(path))
                .sorted()
                .collect();
            let partition = DeltaTree::build_partition(components.as_slice());
            DeltaTree {
                root: partition
            }
        }
    }

    fn parse_path(mut path: Vec<&str>) -> (Vec<PartitionPath>, ParquetDeltaFile) {
        let parquet = DeltaTree::parse_file(path.pop().unwrap());
        let remaining_path = path.into_iter()
            .map(|part| DeltaTree::key_value(part).unwrap())
            .collect();
        (remaining_path, parquet)
    }

    fn key_value(path: &str) -> Option<PartitionPath> {
        if let Some(idx) = path.find('=') {
            Some( PartitionPath { key: &path[0 .. idx], value: &path[idx + 1..] } )
        } else {
            None
        }
    }

    fn build_partition(paths: &[(Vec<PartitionPath>, ParquetDeltaFile)]) -> TreeNode {
        match paths {
            [ first_entry, .. ] => {
                match first_entry.0.as_slice() {
                    [] => { // leaf: no further directory entries
                        let files: Vec<ParquetDeltaFile> = paths.iter()
                            .map(|pf| pf.1 )
                            .collect();
                        TreeNode::FileEntries { files }
                    }
                    first_path=> {
                        let components      = first_path.len();
                        let p1= &first_path[0];
                        let name = p1.key;
                        let mut current_value = p1.value;
                        paths.partition_point()
                        for path in paths {
                            assert_eq!(path.0.len(), components);
                            let &PartitionPath { key, value  } = path.0.get(0).unwrap();
                            assert_eq!(key, name);
                            if value == current_value {
                                // println!("{:?} == {:?} at level {:?}",
                                //
                                // )
                                println!("staying at key: {:?} value: {:?}", key, value);
                            } else {
                                println!("switching partition value @{:?} {:?} -> {:?}",
                                        key, current_value, value);                          }
                            current_value = value;
                        }
                        TreeNode::FileEntries { files: vec![] }
                    }
                }
            }
            [] => TreeNode::FileEntries { files: vec![] }
        }
    }

    fn parse_file(name: &str) -> ParquetDeltaFile {
        let caps = FILENAME_REGEX.captures(name).unwrap();
        let partition = caps["part"].parse::<u32>()
            .unwrap_or_else(|_err| <u32>::max_value());
        let uuid = Uuid::parse_str(&caps["uuid"]).unwrap().as_u128();
        let compression = CompressionType::from_str(&caps["compression"]);

        ParquetDeltaFile { partition, uuid, compression }
    }

}

#[cfg(test)]
mod tests {
    // part-00007-49c0395d-eccb-4882-8f19-bec668752cbe.c000.snappy.parquet
    use pretty_assertions::{assert_eq};
    use super::*; // we're in a submodule (test), bring parent into scope.
    use super::CompressionType::*;

    const F1: &str = "part-00007-00000000-0000-0000-0000-000000000000.c000.snappy.parquet";
    const F2: &str = "part-00007-00000000-0000-0000-0000-000000000001.c000.snappy.parquet";
    const F3: &str = "part-00007-00000000-0000-0000-0000-000000000002.c000.snappy.parquet";
    const F4: &str = "part-00007-00000000-0000-0000-0000-000000000003.c000.snappy.parquet";


    const FE1: ParquetDeltaFile = ParquetDeltaFile { partition: 7, uuid: 0, compression: SNAPPY  };
    const FE2: ParquetDeltaFile = ParquetDeltaFile { partition: 7, uuid: 1, compression: SNAPPY  };
    const FE3: ParquetDeltaFile = ParquetDeltaFile { partition: 7, uuid: 2, compression: SNAPPY  };
    const FE4: ParquetDeltaFile = ParquetDeltaFile { partition: 7, uuid: 3, compression: SNAPPY  };

    #[test]
    fn list_of_files_as_flat_tree() {
        let paths = vec![F1.to_string(), F2.to_string(), F3.to_string(), F4.to_string()];
        let tree = DeltaTree::from_paths(&paths);
        let expected = DeltaTree { root: TreeNode::FileEntries { files: vec![FE1, FE2, FE3, FE4] } };
        assert_eq!(expected, tree);
    }

    #[test]
    fn nested_partitions() {
        let paths = vec![
            "a=1/b=1/".to_string() + F1,
            "a=4/b=2/".to_string() + F2,
            "a=1/b=7/".to_string() + F3,
            "a=4/b=1/".to_string() + F4 ];
        let level_a_1_b = create_leaf_partition(
            "b", vec![ ("1", FE1), ("7", FE3)]);
        let level_a_4_b = create_leaf_partition(
            "b", vec![ ("1", FE4), ("2", FE2)]);
        let root = create_partition(
            "a", vec![("1", level_a_1_b), ("4", level_a_4_b)]);
        let expected = DeltaTree { root };

        let actual = DeltaTree::from_paths(&paths);

        assert_eq!(expected, actual);
    }

    fn single_file_entries(file: ParquetDeltaFile) -> TreeNode {
        TreeNode::FileEntries { files: vec![file] }
    }

    /// test only. helpers to build a hashmap.
    fn create_leaf_partition(name: &str, entries: Vec<(&str, ParquetDeltaFile)>) -> TreeNode {
        let mut values = HashMap::new();
        entries.into_iter().for_each(|(k, v)| {
            values.insert(k.to_string(), single_file_entries(v));
        });
        TreeNode::Partition { name: name.to_string(), values }
    }

    fn create_partition(name: &str, entries: Vec<(&str, TreeNode)>) -> TreeNode {
        let mut values = HashMap::new();
        entries.into_iter().for_each(|(k, v)| {
            values.insert(k.to_string(), v);
        });
        TreeNode::Partition { name: name.to_string(), values }
    }

    #[test]
    fn test_uuid_parse() -> () {
        assert_eq!(10, Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap().as_u128());
        assert_eq!(10, Uuid::parse_str("00000000-0000-0000-0000-00000000000A").unwrap().as_u128());
    }

    #[test]
    fn test_file_name_parse() {
        let name = "part-00009-477077ae-1429-4633-b07a-0c0cb75caf55.c000.snappy.parquet";
        let entry = DeltaTree::parse_file(&name);
        assert_eq!(entry, ParquetDeltaFile { partition: 9,
            uuid: 94959152347567637375526247419927637845,
            compression: SNAPPY
        } );
    }

    #[test]
    fn test_regex_filename() {
        let name = "part-00009-477077ae-1429-4633-b07a-0c0cb75caf55.c000.snappy.parquet";

        let regex = Regex::new("part-(?P<part>\\d{5})-\
                (?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-\
                [0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.c000\\.\
                (?P<compression>(snappy|gzip|none)).parquet").unwrap();
        let caps = regex.captures(name).unwrap();
        assert_eq!(&caps["part"], "00009");
        assert_eq!(&caps["uuid"], "477077ae-1429-4633-b07a-0c0cb75caf55");
        assert_eq!(&caps["compression"], "snappy");
    }

    #[test]
    fn test_key_value() {
        assert_eq!(DeltaTree::key_value("a=13"), Some(PartitionPath { key: "a", value: "13"}));
        assert_eq!(DeltaTree::key_value("askaban"), None);
        assert_eq!(DeltaTree::key_value("some-key=some-value-with-=-sign-in-the-middle"),
                   Some(
                       PartitionPath {
                           key: "some-key",
                           value: "some-value-with-=-sign-in-the-middle"
                       }))
    }
}