use rustc_hash::FxHashMap;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TagData {
    idx: u32,
    count: u32,
}

#[derive(Debug, Clone)]
pub struct Tags {
    tag_set: FxHashMap<Rc<str>, TagData>,
    vec: Vec<Rc<str>>,
}

impl Tags {
    pub fn new() -> Tags {
        Tags {
            tag_set: FxHashMap::default(),
            vec: Vec::new(),
        }
    }

    pub fn add_or_increment(&mut self, tag: &str) -> u32 {
        if let Some(tag_data) = self.tag_set.get_mut(tag) {
            tag_data.count += 1;
            tag_data.idx
        } else {
            let tag: Rc<str> = Rc::from(tag);
            self.vec.push(Rc::clone(&tag));
            let idx = self.vec.len() as u32 - 1;
            let count = 1;
            self.tag_set.insert(Rc::clone(&tag), TagData { idx, count });
            idx
        }
    }

    pub fn get_idx(&self, tag: &str) -> Option<u32> {
        if self.tag_set.contains_key(tag) {
            Some(self.tag_set[tag].idx)
        } else {
            None
        }
    }

    pub fn get_name(&self, idx: u32) -> Option<&str> {
        if idx < self.vec.len() as u32 {
            Some(&self.vec[idx as usize])
        } else {
            None
        }
    }

    pub fn get_count(&self, name: &str) -> Option<u32> {
        if self.tag_set.contains_key(name) {
            Some(self.tag_set[name].count)
        } else {
            None
        }
    }

    pub fn get_count_idx(&self, idx: u32) -> Option<u32> {
        let name = self.get_name(idx);
        match name {
            Some(name) => {
                let count = self.get_count(name);
                Some(count.unwrap())
            }
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }
}

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser_tags = serializer.serialize_struct("Tags", 2)?;

        let tag_ser_ser: FxHashMap<&str, &TagData> =
            self.tag_set.iter().map(|(k, v)| (&**k, v)).collect();

        let vec_ser: Vec<&str> = self.vec.iter().map(|v| &**v).collect();

        ser_tags.serialize_field("tag_set", &tag_ser_ser)?;
        ser_tags.serialize_field("vec", &vec_ser)?;
        ser_tags.end()
    }
}

impl<'de> Deserialize<'de> for Tags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            tag_set: FxHashMap<String, TagData>,
            vec: Vec<String>,
        }

        let helper = Helper::deserialize(deserializer)?;
        let mut tag_set: FxHashMap<Rc<str>, TagData> = FxHashMap::default();
        let mut vec: Vec<Rc<str>> = Vec::new();
        for tag in helper.vec {
            let tag_data = helper.tag_set.get(&tag).unwrap().to_owned();
            let tag: Rc<str> = Rc::from(tag.as_str());
            vec.push(Rc::clone(&tag));
            tag_set.insert(Rc::clone(&tag), tag_data);
        }

        Ok(Tags { tag_set, vec })
    }
}
