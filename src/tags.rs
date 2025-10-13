use rustc_hash::FxHashMap;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug)]
struct TagData {
    name: Rc<str>,
    count: u32,
}

#[derive(Debug, Clone)]
pub struct Tags {
    tag_set: FxHashMap<Rc<str>, usize>,
    vec: Vec<TagData>,
}

impl Tags {
    pub fn new() -> Tags {
        Tags {
            tag_set: FxHashMap::default(),
            vec: Vec::new(),
        }
    }

    pub fn add_or_increment(&mut self, tag: &str) -> usize {
        if let Some(idx) = self.tag_set.get(tag) {
            self.vec[*idx].count += 1;
            idx.to_owned()
        } else {
            let tag_name: Rc<str> = Rc::from(tag);
            let tag_data = TagData {
                name: Rc::clone(&tag_name),
                count: 1
            };
            self.vec.push(tag_data);
            let idx = self.vec.len() -1;
            self.tag_set.insert(Rc::clone(&tag_name), idx);
            idx
        }
    }

    pub fn get_idx(&self, tag: &str) -> Option<usize> {
        if self.tag_set.contains_key(tag) {
            Some(self.tag_set.get(tag).unwrap().to_owned())
        } else {
            None
        }
    }

    pub fn get_name(&self, idx: usize) -> Option<&str> {
        if idx < self.vec.len() {
            Some(&self.vec[idx].name)
        } else {
            None
        }
    }

    pub fn get_count(&self, name: &str) -> Option<u32> {
        if self.tag_set.contains_key(name) {
            let idx = self.tag_set[name];
            Some(self.vec[idx].count)
        } else {
            None
        }
    }

    pub fn get_count_idx(&self, idx: usize) -> Option<u32> {
        if idx < self.vec.len() {
            Some(self.vec[idx].count)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }
}

impl Serialize for TagData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser_tag_data =
            serializer.serialize_struct("TagData", 2)?;

        let name = &*self.name;
        let count = self.count;

        ser_tag_data.serialize_field("name", name)?;
        ser_tag_data.serialize_field("count", &count)?;
        ser_tag_data.end()
    }
}

impl<'de> Deserialize<'de> for TagData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            name:  String,
            count: u32,
        }

        let helper = Helper::deserialize(deserializer)?;
        let name: Rc<str> = Rc::from(helper.name.as_str().to_owned());
        let count: u32 = helper.count;

        Ok(TagData { name, count })
    }
}

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser_tags =
            serializer.serialize_struct("Tags", 2)?;

        let tag_set_ser: FxHashMap<&str, usize> = self.tag_set.iter()
            .map(|(k, v)| (&**k, *v))
            .collect();

        let vec_ser: Vec<TagData> = self.vec.iter()
            .map(|td| td.clone())
            .collect();


        ser_tags.serialize_field("tag_set", &tag_set_ser)?;
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
            tag_set: FxHashMap<String, usize>,
            vec: Vec<TagData>,
        }

        let helper = Helper::deserialize(deserializer)?;
        let mut tag_set: FxHashMap<Rc<str>, usize> = FxHashMap::default();
        for (idx, tag) in helper.vec.iter().enumerate() {
            let tag: Rc<str> = Rc::clone(&helper.vec[idx].name);
            tag_set.insert(Rc::clone(&tag), idx);
        }

        Ok(Tags {
            tag_set,
            vec: helper.vec
        })
    }
}
