use deltalake;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct DeltaTree {
    pub root: TreeNode,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TreeNode {
    /// a partition is a key and a map of all its values to the next lower level in the tree.
    Partition {
        name: String,                      // the key / column name of the partition
        values: HashMap<String, TreeNode>, // partition values mapped to the content
    },

    /// represent the contents of a single leaf directory: a set of parquet files.
    FileEntries { files: Vec<ParquetDeltaFile> },
}

/// a single parquet file, represented in a compact partion / uuid / compression triple.
/// TODO: figure out if other name components are variable, e.g. `c000`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ParquetDeltaFile {
    partition: u32,
    uuid: u128,
    cluster: u8,
    compression: CompressionType,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionPath<'a> {
    key: &'a str,
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
            "gzip" => CompressionType::GZIP,
            "none" => CompressionType::NONE,
            _ => panic!("unexpected compression name, {}", s),
        }
    }

    fn to_string(&self) -> &str {
        match self {
            CompressionType::GZIP => "gzip",
            CompressionType::SNAPPY => "snappy",
            CompressionType::NONE => "none",
        }
    }
}

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new(
        "^part-(?P<part>\\d{5})-\
                (?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-\
                [0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.c(?P<c>\\d{3})\\.\
                (?P<compression>(snappy|gzip|none)).parquet"
    )
    .unwrap();
}

impl ParquetDeltaFile {
    fn from_string(name: &str) -> ParquetDeltaFile {
        if let Some(caps) = FILENAME_REGEX.captures(name) {
            let partition = caps["part"]
                .parse::<u32>()
                .unwrap_or_else(|_err| <u32>::max_value());
            let uuid = Uuid::parse_str(&caps["uuid"]).unwrap().as_u128();
            let cluster = caps["c"].parse().unwrap();
            let compression = CompressionType::from_str(&caps["compression"]);

            ParquetDeltaFile {
                partition,
                uuid,
                cluster,
                compression,
            }
        } else {
            panic!("unable to parse '{}'", name)
        }
    }
    fn name(&self) -> String {
        let uuid = Uuid::from_u128(self.uuid);
        format!(
            "part-{:05}-{}.c{:03}.{}.parquet",
            self.partition,
            uuid,
            self.cluster,
            self.compression.to_string()
        )
    }
}

impl DeltaTree {
    pub fn new(delta_table: &deltalake::DeltaTable) -> DeltaTree {
        DeltaTree::from_paths(delta_table.get_files())
    }

    pub fn from_paths(input_files: &Vec<String>) -> DeltaTree {
        if input_files.is_empty() {
            DeltaTree {
                root: TreeNode::FileEntries { files: vec![] },
            }
        } else {
            let components: Vec<(Vec<PartitionPath>, ParquetDeltaFile)> = input_files
                .iter()
                .map(|f| f.split('/').collect())
                .map(|path| DeltaTree::parse_path(path))
                .sorted()
                .collect();
            let partition = DeltaTree::build_partition(components.as_slice(), 0);
            DeltaTree { root: partition }
        }
    }

    pub fn files(&self) -> Vec<String> {
        fn files_in_subtree<'a>(prefix: &'a str, node: &TreeNode) -> Vec<String> {
            match node {
                TreeNode::FileEntries { files } => files
                    .iter()
                    .map(|f| format!("{}{}", prefix, f.name()))
                    .collect(),
                TreeNode::Partition { name, values } => values
                    .iter()
                    .flat_map(|(value, node)| {
                        let sub_prefix = format!("{}{}={}/", prefix, name, value);
                        files_in_subtree(&sub_prefix, node)
                    })
                    .collect(), // vec![],
            }
        }

        files_in_subtree("", &self.root)
    }

    fn parse_path(mut path: Vec<&str>) -> (Vec<PartitionPath>, ParquetDeltaFile) {
        let parquet = ParquetDeltaFile::from_string(path.pop().unwrap());
        let remaining_path = path
            .into_iter()
            .map(|part| DeltaTree::key_value(part).unwrap())
            .collect();
        (remaining_path, parquet)
    }

    fn key_value(path: &str) -> Option<PartitionPath> {
        if let Some(idx) = path.find('=') {
            Some(PartitionPath {
                key: &path[0..idx],
                value: &path[idx + 1..],
            })
        } else {
            None
        }
    }

    fn build_partition(paths: &[(Vec<PartitionPath>, ParquetDeltaFile)], level: usize) -> TreeNode {
        match paths {
            [first_entry, ..] => {
                if let Some(p1) = first_entry.0.get(level) {
                    let name = p1.key;
                    let mut current_value = p1.value;
                    let mut current_index = 0;
                    let mut children: HashMap<String, TreeNode> = HashMap::new();
                    // paths.partition_point()
                    for (idx, path) in paths.iter().enumerate() {
                        assert_eq!(path.0.len(), first_entry.0.len());
                        let &PartitionPath { key, value } = path.0.get(level).unwrap();
                        assert_eq!(key, name);
                        if value != current_value {
                            let child =
                                DeltaTree::build_partition(&paths[current_index..idx], level + 1);
                            children.insert(current_value.to_string(), child);
                            current_value = value;
                            current_index = idx;
                        }
                    }
                    let last_child = DeltaTree::build_partition(&paths[current_index..], level + 1);
                    children.insert(current_value.to_string(), last_child);
                    TreeNode::Partition {
                        name: name.to_string(),
                        values: children,
                    }
                } else {
                    let files: Vec<ParquetDeltaFile> = paths.iter().map(|pf| pf.1).collect();
                    TreeNode::FileEntries { files }
                }
            }
            [] => TreeNode::FileEntries { files: vec![] },
        }
    }
}

#[cfg(test)]
mod tests {
    // part-00007-49c0395d-eccb-4882-8f19-bec668752cbe.c000.snappy.parquet
    use super::CompressionType::*;
    use super::*; // we're in a submodule (test), bring parent into scope.
    use pretty_assertions::assert_eq;

    const F1: &str = "part-00007-00000000-0000-0000-0000-000000000000.c000.snappy.parquet";
    const F2: &str = "part-00007-00000000-0000-0000-0000-000000000001.c001.snappy.parquet";
    const F3: &str = "part-00007-00000000-0000-0000-0000-000000000002.c002.snappy.parquet";
    const F4: &str = "part-00007-00000000-0000-0000-0000-000000000003.c003.snappy.parquet";

    const FE1: ParquetDeltaFile = ParquetDeltaFile {
        partition: 7,
        uuid: 0,
        cluster: 0,
        compression: SNAPPY,
    };
    const FE2: ParquetDeltaFile = ParquetDeltaFile {
        partition: 7,
        uuid: 1,
        cluster: 1,
        compression: SNAPPY,
    };
    const FE3: ParquetDeltaFile = ParquetDeltaFile {
        partition: 7,
        uuid: 2,
        cluster: 2,
        compression: SNAPPY,
    };
    const FE4: ParquetDeltaFile = ParquetDeltaFile {
        partition: 7,
        uuid: 3,
        cluster: 3,
        compression: SNAPPY,
    };

    #[test]
    fn list_of_files_as_flat_tree() {
        let paths = vec![
            F1.to_string(),
            F2.to_string(),
            F3.to_string(),
            F4.to_string(),
        ];
        let tree = DeltaTree::from_paths(&paths);
        let expected = DeltaTree {
            root: TreeNode::FileEntries {
                files: vec![FE1, FE2, FE3, FE4],
            },
        };
        assert_eq!(expected, tree);
    }

    fn tree_round_trip(mut files: Vec<String>) -> () {
        let tree = DeltaTree::from_paths(&files);
        let mut files_from_tree = tree.files();

        files.sort();
        files_from_tree.sort();
        assert_eq!(files, files_from_tree);
    }

    #[test]
    fn tree_parse_nested_partitions() {
        let nested_paths: Vec<String> = vec![
            "a=1/b=1/".to_string() + F1,
            "a=4/b=2/".to_string() + F2,
            "a=1/b=7/".to_string() + F3,
            "a=4/b=1/".to_string() + F4,
        ];

        let level_a_1_b = create_leaf_partition("b", vec![("1", FE1), ("7", FE3)]);
        let level_a_4_b = create_leaf_partition("b", vec![("1", FE4), ("2", FE2)]);
        let root = create_partition("a", vec![("1", level_a_1_b), ("4", level_a_4_b)]);
        let expected = DeltaTree { root };

        let actual = DeltaTree::from_paths(&nested_paths);

        assert_eq!(expected, actual);
    }

    #[test]
    fn file_name_round_trip() {
        assert_eq!(ParquetDeltaFile::from_string(F1).name(), F1);
        assert_eq!(ParquetDeltaFile::from_string(F2).name(), F2);
        assert_eq!(ParquetDeltaFile::from_string(F3).name(), F3);
        assert_eq!(ParquetDeltaFile::from_string(F4).name(), F4);
    }

    #[test]
    fn flat_table_round_trip() {
        let paths = vec![
            F1.to_string(),
            F2.to_string(),
            F3.to_string(),
            F4.to_string(),
        ];
        tree_round_trip(paths);
    }

    #[test]
    fn nested_partitions_round_trip() {
        let mut nested_paths: Vec<String> = vec![
            "a=1/b=1/".to_string() + F1,
            "a=4/b=2/".to_string() + F2,
            "a=1/b=7/".to_string() + F3,
            "a=4/b=1/".to_string() + F4,
        ];
        tree_round_trip(nested_paths);
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
        TreeNode::Partition {
            name: name.to_string(),
            values,
        }
    }

    fn create_partition(name: &str, entries: Vec<(&str, TreeNode)>) -> TreeNode {
        let mut values = HashMap::new();
        entries.into_iter().for_each(|(k, v)| {
            values.insert(k.to_string(), v);
        });
        TreeNode::Partition {
            name: name.to_string(),
            values,
        }
    }

    #[test]
    fn test_uuid_parse() -> () {
        assert_eq!(
            10,
            Uuid::parse_str("00000000-0000-0000-0000-00000000000a")
                .unwrap()
                .as_u128()
        );
        assert_eq!(
            10,
            Uuid::parse_str("00000000-0000-0000-0000-00000000000A")
                .unwrap()
                .as_u128()
        );
    }

    #[test]
    fn test_file_name_parse() {
        let name = "part-00009-477077ae-1429-4633-b07a-0c0cb75caf55.c177.snappy.parquet";
        let entry = ParquetDeltaFile::from_string(&name);
        assert_eq!(
            entry,
            ParquetDeltaFile {
                partition: 9,
                uuid: 94959152347567637375526247419927637845,
                cluster: 177,
                compression: SNAPPY
            }
        );
    }

    #[test]
    fn test_regex_filename() {
        let name = "part-00009-477077ae-1429-4633-b07a-0c0cb75caf55.c003.snappy.parquet";
        let caps = FILENAME_REGEX.captures(name).unwrap();
        assert_eq!(&caps["part"], "00009");
        assert_eq!(&caps["uuid"], "477077ae-1429-4633-b07a-0c0cb75caf55");
        assert_eq!(&caps["c"], "003");
        assert_eq!(&caps["compression"], "snappy");
    }

    #[test]
    fn test_key_value() {
        assert_eq!(
            DeltaTree::key_value("a=13"),
            Some(PartitionPath {
                key: "a",
                value: "13"
            })
        );
        assert_eq!(DeltaTree::key_value("askaban"), None);
        assert_eq!(
            DeltaTree::key_value("some-key=some-value-with-=-sign-in-the-middle"),
            Some(PartitionPath {
                key: "some-key",
                value: "some-value-with-=-sign-in-the-middle"
            })
        )
    }
}
