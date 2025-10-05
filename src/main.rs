use csv::ReaderBuilder;
use std::error::Error;
use std::collections::HashMap;

#[derive(Debug)]
struct Tag {
    idx: u32,
    count: u32,
}

#[derive(Debug)]
struct Tags {
    tag_set: HashMap<String, Tag>,
    vec: Vec<String>
}

impl Tags {
    pub fn new() -> Tags {
        Tags {
            tag_set: HashMap::new(),
            vec: Vec::new(),
        }
    }

    pub fn add_or_increment(& mut self, tag: &str) {
        let tag = tag.to_string();
        if (self.tag_set.contains_key(&tag)) {
            self.tag_set.get_mut(&tag).unwrap().count += 1;
        } else {
            self.vec.push(tag.clone());
            let idx = self.vec.len() as u32 - 1;
            let count = 1;
            self.tag_set.insert(tag, Tag { idx, count });
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }
}

struct CSR {
    n_rows: usize,
    n_cols: usize,
    n_nz: usize,
    row_ptr: Vec<usize>,
    col_ind: Vec<usize>,
    val: Vec<f64>,
}

impl CSR {
    fn new() -> CSR {
        CSR {
            n_rows: 0,
            n_cols: 0,
            n_nz: 0,
            row_ptr: Vec::new(),
            col_ind: Vec::new(),
            val: Vec::new(),
        }
    }
}

fn read_csv(path: &str) -> Result<Tags, Box<dyn Error>> {
    use serde::Deserialize;
    #[derive(Debug,Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;

    let mut tags = Tags::new();

    let mut i = 0;
    for line in reader.deserialize() {
        let row: Row = line?;
        let post_tags: Vec<&str> = row.tag_string
            .split_whitespace()
            .collect();
        for tag in post_tags {
            tags.add_or_increment(tag);
        }
        i += 1;
    }

    // println!("{:?}", tags.tag_set);
    println!("{}", i);

    Ok(tags)
}

fn main() {
    read_csv("lib/posts.csv");
}
