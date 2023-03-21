use std::collections::HashMap;

pub mod partition;

fn main() {
    println!("Hello world");
    //these are only here so that the test in partition.rs get executed at the moment
    //so just temporary. One can also add the test here but that wouldn't make much sense
    let cache = &mut partition::Cache {
        map: HashMap::new(),
        capacity: 20,
    };
    let entry = partition::EntryVal {
        value: String::from("Value"),
    };
}