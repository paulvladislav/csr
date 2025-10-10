mod nmpi;
mod read_write;

use csv::ReaderBuilder;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::ops::Add;
use std::rc::Rc;
use std::string::String;
use std::thread;
use std::time::Instant;

use csr_matrix::CSR;
use log::log;
use crate::nmpi::{calculate_npmi, get_co_counts};

#[derive(Debug)]
struct TagData {
    idx: u32,
    count: u32,
}

#[derive(Debug)]
struct Tags {
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
        if (self.tag_set.contains_key(tag)) {
            Some(self.tag_set[tag].idx)
        } else {
            None
        }
    }

    pub fn get_name(&self, idx: u32) -> Option<&str> {
        if (idx < self.vec.len() as u32) {
            Some(&self.vec[idx as usize])
        } else {
            None
        }
    }

    pub fn get_count(&self, name: &str) -> Option<u32> {
        if (self.tag_set.contains_key(name)) {
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

fn main() {
    let (n_posts, tags, posts_tags_idxs) = read_write::read_posts("data/posts.csv").unwrap();
    let co_counts_matrix = get_co_counts(n_posts, &tags, posts_tags_idxs). unwrap();
    let npmi_matrix = calculate_npmi(n_posts, &tags, &co_counts_matrix);
    
}
