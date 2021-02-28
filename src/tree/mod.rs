use deltalake;
use uuid::Uuid;
use regex::Regex;
use lazy_static::lazy_static;
use std::collections::HashMap;

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
#[derive(Debug, PartialEq, Eq)]
struct ParquetDeltaFile {
    partition:        u32,
    uuid:             u128,
    compression: CompressionType,
}

#[derive(Debug, PartialEq, Eq)]
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
    static ref FILENAME_REGEX: Regex = Regex::new("part-(?P<part>\\d{5})-\
                (?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-\
                [0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.c000\\.\
                (?P<compression>(snappy|gzip|none)).parquet").unwrap();
}

impl DeltaTree {

    pub fn new(delta_table: &deltalake::DeltaTable) -> DeltaTree {
        DeltaTree::from_paths(delta_table.get_files())
    }

    pub fn from_paths(input_files: &Vec<String>) -> DeltaTree {
        let files: Vec<ParquetDeltaFile> = input_files.iter()
            .map(|f| DeltaTree::parse_file(f))
            .collect();
        DeltaTree {
            root: TreeNode::FileEntries { files }
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
    use super::TreeNode::*;

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
        assert_eq!(13, 14, "lala");
        // val part_b_a1 = Partition { };
        // let expected = DeltaTree {
        //     root: vec![]
        // };
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
}