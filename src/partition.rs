use std::collections::HashMap;
use std::io;

#[derive(Debug)]
pub struct EntryVal {
    pub value: String,
}

impl EntryVal {
    pub fn new(value: String)-> Self {
        EntryVal {
            value,
        }
    }
    pub fn update_value(&mut self, new_value: String){
        self.value = new_value;
    }
}

pub struct Cache<'a> {
    pub map: HashMap<&'a str, EntryVal>, 
    pub capacity: u32,
}   

impl <'a> Cache <'a>{
    pub fn new(map: HashMap<&'a str, EntryVal>, capacity: u32)-> Self {
        Self {
            map,
            capacity,
        }
    }

    pub fn put(&mut self, key: &'a str, new_entry_value: String) {
        if self.map.contains_key(key) {
            let new_entry_val = EntryVal{
                value: new_entry_value,
            };
            self.map.insert(key, new_entry_val);
        } else {
            let entry = EntryVal {
                value: new_entry_value, 
            };
            self.map.insert(key, entry);
        }
    }
    
    pub fn get(&self, key: &'a str)->io::Result<Option<&EntryVal>> {
        if self.map.contains_key(&key) {
            let result = self.map.get(&key).unwrap(); 
            println!("Found value {}", result.value);
            return Ok(Some(result));
        }else {
            println!("No value found!");
            return Ok(None);
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
        let entry = EntryVal {
            value: String::from("Value"),
        };
        cache.map.insert("Key", entry);
        let res = cache.get("Key").unwrap().unwrap();
        assert_eq!(&res.value, "Value");
    }
}