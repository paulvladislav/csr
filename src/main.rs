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
        if (idx < self.vec.len() as u32 - 1) {
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

fn read_csv(path: &str) -> Result<(usize, Tags, CSR), Box<dyn Error>> {
    const N_CHUNCK: usize = 8;

    use serde::Deserialize;
    #[derive(Debug, Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;

    let mut tags = Tags::new();

    let read_start = Instant::now();

    let mut n_posts = 0;
    let mut posts_tag_idxs: Vec<Vec<u32>> = Vec::new();
    for line in reader.deserialize() {
        let row: Row = line?;
        let tag_idxs: Vec<u32> = row
            .tag_string
            .split_whitespace()
            .map(|tag| tags.add_or_increment(tag))
            .collect();

        posts_tag_idxs.push(tag_idxs);

        n_posts += 1;
    }

    println!("Read {} posts", n_posts);
    println!("Read {} tags", tags.len());
    // println!("mammal: {:#?}", tags.get_idx("mammal").unwrap());
    // println!("domestic cat: {:#?}", tags.get_idx("domestic_cat").unwrap());

    let n_tags = tags.len();
    let mut handles = Vec::new();
    for chunk in posts_tag_idxs.chunks((n_posts / N_CHUNCK) + 1) {
        let chunk = chunk.to_vec();
        let handle = thread::spawn(move || {
            let mut co_counts: FxHashMap<(usize, usize), f32> = FxHashMap::default();

            for post_tag_idx in chunk {
                for i in 0..post_tag_idx.len() {
                    for j in (i + 1)..post_tag_idx.len() {
                        let a = post_tag_idx[i] as usize;
                        let b = post_tag_idx[j] as usize;
                        *co_counts.entry((a, b)).or_insert(0.0) += 1.0;
                        *co_counts.entry((b, a)).or_insert(0.0) += 1.0;
                    }
                }
            }

            let local_co_count_matrix = CSR::from_fxhash(&co_counts, n_tags, n_tags);
            local_co_count_matrix
        });
        handles.push(handle);
    }

    let mut co_count_matrix = CSR::new(n_tags, n_tags);
    for (i, handle) in handles.into_iter().enumerate() {
        let slice = handle.join().unwrap();
        println!("summing slice {} of {N_CHUNCK}", i + 1);
        co_count_matrix.add_in_place(&slice)?;
    }

    let elapsed = read_start.elapsed();
    println!("reading took {:?}", elapsed);

    Ok((n_posts, tags, co_count_matrix))
}

fn calculate_npmi(n_posts: usize, tags: &Tags, co_count_matrix: &CSR) -> CSR {
    let mut npmi_triple: Vec<(usize, usize, f32)> = Vec::new();
    let post_freq = 1.0 / n_posts as f32;
    for (row, col, val) in co_count_matrix {
    //     let p_x: f32;
    //     match tags.get_count_idx(row as u32) {
    //         Some(count) => {
    //             p_x = count as f32 * post_freq;
    //         }
    //         None => {
    //             panic!("row {row} col {col} val {val}");
    //         }
    //     }
    //     let p_y: f32;
    //     match tags.get_count_idx(row as u32) {
    //         Some(count) => {
    //             p_y = count as f32 * post_freq;
    //         }
    //         None => {
    //             panic!("row {row} col {col} val {val}");
    //         }
    //     }
    //     let p_xy = val as f32 * post_freq;
    //     let npmi_xy = (p_xy.log2() - p_x.log2() - p_y.log2()) / -p_xy.log2();
    //     if npmi_xy > 0.0 {
    //         npmi_triple.push((row, col, npmi_xy));
    //     }
        println!("row: {:?}, col: {:?}, val: {val}", row, col);
    }
    // let npmi_matrix = CSR::from_triplet(&npmi_triple, tags.len(), tags.len());
    // npmi_matrix
    CSR::new(3, 3)
}

fn main() {
    let result = read_csv("data/posts_egs.csv");
    match result {
        Err(e) => println!("Error: {:?}", e),
        Ok((n_posts, tags, co_count_matrix)) => {
            // let cat_idx = tags.get_idx("domestic_cat").unwrap() as usize;
            // let mammal_idx = tags.get_idx("mammal").unwrap() as usize;
            // println!("{:?}", co_count_matrix.value(cat_idx, mammal_idx));
            // println!("{:?}", co_count_matrix.value(mammal_idx, cat_idx));
            println!(
                "size: {:.2} GB",
                co_count_matrix.size() as f32 / 1_000_000_000.0
            );

            let npmi = calculate_npmi(n_posts, &tags, &co_count_matrix);
            println!("npmi: {:?}", npmi);
        }
    }
}
