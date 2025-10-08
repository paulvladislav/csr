use std::string::String;
use csv::{Position, ReaderBuilder};
use itertools::Itertools;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::arch::x86_64::_mm_round_ss;
use std::collections::HashMap;
use std::error::Error;
use std::rc::Rc;
use std::time::Instant;
use serde::__private228::de::Content::String as SerdeString;
use rayon::prelude::*;

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

#[derive(Debug)]
struct CSR {
    n_rows: usize,
    n_cols: usize,
    n_nz: usize,
    row_ptr: Vec<usize>,
    col_idx: Vec<usize>,
    val: Vec<f32>,
}

impl CSR {
    pub fn from_fxhash(
        values: &FxHashMap<(usize, usize), f32>,
        n_rows: usize,
        n_cols: usize,
    ) -> CSR {
        let mut rows: Vec<Vec<(usize, f32)>> = vec![vec![]; n_rows];
        for (&(r, c), &v) in values {
            rows[r].push(((c, v)));
        }
        for row in rows.iter_mut() {
            row.sort_by_key(|(c, _)| *c);
        }

        let mut row_ptr: Vec<usize> = Vec::with_capacity(n_rows + 1);
        let mut col_idx: Vec<usize> = Vec::new();
        let mut val: Vec<f32> = Vec::new();
        let mut n_nz = 0;

        row_ptr.push(0);
        for row in &rows {
            for &(c, v) in row {
                col_idx.push(c);
                val.push(v);
                n_nz += 1;
            }
            row_ptr.push(col_idx.len());
        }

        CSR {
            n_rows,
            n_cols,
            n_nz,
            row_ptr,
            col_idx,
            val,
        }
    }

    pub fn value(&self, row: usize, col: usize) -> Option<f32> {
        if (row > self.n_rows || col > self.n_cols) {
            return None;
        }
        let row_start = self.row_ptr[row];
        let row_end = self.row_ptr[row + 1];
        for i in row_start..row_end {
            if self.col_idx[i] == col {
                return Some(self.val[i].clone());
            }
        }
        Some(0.0)
    }

    pub fn size(&self) -> usize {
        (self.n_rows + 1) * size_of::<usize>()
            + self.n_nz * size_of::<usize>()
            + self.n_nz * size_of::<u32>()
            + size_of::<Vec<f32>>()
            + size_of::<Vec<usize>>() * 2
            + size_of::<f32>()
            + size_of::<usize>() * 2
    }
}

fn read_csv(path: &str) -> Result<(Tags, CSR), Box<dyn Error>> {
    use serde::Deserialize;
    #[derive(Debug, Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;

    let mut tags = Tags::new();
    let mut co_counts: FxHashMap<(usize, usize), f32> = FxHashMap::default();

    let read_start = Instant::now();

    let mut n_posts = 0;

    const CHUNK_SIZE: usize = 1;
    let mut chunks: Vec<Vec<Vec<u32>>> = Vec::new();
    let mut current_chunk: Vec<Vec<u32>> = Vec::with_capacity(CHUNK_SIZE);
    for line in reader.deserialize() {
        let row: Row = line?;
        let post_tags_idxs: Vec<u32> = row
            .tag_string
            .split_whitespace()
            .map(|tag| tags.add_or_increment(tag))
            .collect();

        current_chunk.push(post_tags_idxs);
        if current_chunk.len() >= CHUNK_SIZE {
            chunks.push(current_chunk);
            current_chunk = Vec::with_capacity(CHUNK_SIZE);
        }

        // for i in 0..post_tags_idxs.len() {
        //     for j in (i+1)..post_tags_idxs.len() {
        //         let a = post_tags_idxs[i] as usize;
        //         let b = post_tags_idxs[j] as usize;
        //         *co_counts.entry((a, b)).or_insert(0.0) += 1.0;
        //         *co_counts.entry((b, a)).or_insert(0.0) += 1.0;
        //     }
        // }

        n_posts += 1;
    }
    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    let co_counts: FxHashMap<(usize, usize), f32> = chunks
        .into_par_iter()
        .map(|chunk: Vec<Vec<u32>>| {
            let mut chunk_co_counts: FxHashMap<(usize, usize), f32>= FxHashMap::default();
            for post_tag_idxs in chunk {
                let n_tag_idxs = post_tag_idxs.len();
                for i in 0..n_tag_idxs {
                    for j in (i+1)..n_tag_idxs {
                        let a = post_tag_idxs[i] as usize;
                        let b = post_tag_idxs[j] as usize;
                        *chunk_co_counts.entry((a,b)).or_insert(0.0) += 1.0;
                        *chunk_co_counts.entry((b,a)).or_insert(0.0) += 1.0;
                    }
                }
            }
            chunk_co_counts
        })
        .reduce(
            || FxHashMap::default(),
            |mut acc, current| {
                for (k, v) in current {
                    *acc.entry(k).or_insert(0.0) += v;
                }
                acc
            }
        );

    let elapsed = read_start.elapsed();
    println!("reading took {:?}", elapsed);

    println!("mammal: {:#?}", tags.get_idx("mammal").unwrap());
    println!("cat: {:#?}", tags.get_idx("cat").unwrap());

    let co_count_matrix = CSR::from_fxhash(&co_counts, tags.len(), tags.len());

    Ok((tags, co_count_matrix))
}

fn main() {
    let result = read_csv("lib/posts_egs.csv");
    match result {
        Err(e) => println!("Error: {:?}", e),
        Ok((_, co_count_matrix)) => {
            println!("{:?}", co_count_matrix.value(2, 0));
            println!("{:?}", co_count_matrix.value(0, 2));
            println!("{:?}", co_count_matrix.n_nz);
            println!("{:?}", co_count_matrix.size());
        }
    }
}
