use std::collections::HashMap;

#[derive(Debug)]
pub struct EntryVal {
    pub value: String,
}

impl EntryVal {
    pub fn new(value: String) -> Self {
        EntryVal { value }
    }
    pub fn update_value(&mut self, new_value: String) {
        self.value = new_value;
    }
}

pub struct Cache<'a> {
    pub map: HashMap<&'a str, EntryVal>,
    pub capacity: u32,
}

impl<'a> Cache<'a> {
    pub fn new(map: HashMap<&'a str, EntryVal>, capacity: u32) -> Self {
        Self { map, capacity }
    }

    pub fn put(&mut self, key: &'a str, new_entry_value: String) {
        let entry = EntryVal {
            value: new_entry_value,
        };
        self.map.insert(key, entry);
    }

    pub fn get(&self, key: &'a str) -> Option<&EntryVal> {
        if self.map.contains_key(&key) {
            let result = self.map.get(&key).unwrap();
            println!("Found value {}", result.value);
            Some(result)
        } else {
            println!("No value found!");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_if_value_was_added_to_cache() {
        let cache = &mut Cache {
            map: HashMap::new(),
            capacity: 20,
        };
        cache.put("Key", String::from("Value"));
        let res = cache.get("Key").unwrap();
        assert_eq!(&res.value, "Value");
    }
}
