use std::arch::x86_64::_mm_round_ss;
use csv::{Position, ReaderBuilder};
use std::error::Error;
use std::collections::HashMap;
use itertools::Itertools;

#[derive(Debug)]
struct TagData {
    idx: u32,
    count: u32,
}

#[derive(Debug)]
struct Tags {
    tag_set: HashMap<Box<str>, TagData>,
    vec: Vec<Box<str>>
}

impl Tags {
    pub fn new() -> Tags {
        Tags {
            tag_set: HashMap::new(),
            vec: Vec::new(),
        }
    }

    pub fn add_or_increment(& mut self, tag: &str) {
        if (self.tag_set.contains_key(tag)) {
            self.tag_set.get_mut(tag).unwrap().count += 1;
        } else {
            let tag: Box<str> = Box::from(tag);
            self.vec.push(tag.clone());
            let idx = self.vec.len() as u32 - 1;
            let count = 1;
            self.tag_set.insert(tag, TagData { idx, count });
        }
    }

    pub fn get_idx(& self, tag: &str) -> Option<u32> {
        if (self.tag_set.contains_key(tag)) {
            Some(self.tag_set[tag].idx)
        } else {
            None
        }
    }

    pub fn get_name(& self, idx: u32) -> Option<&str> {
        if (idx < self.vec.len() as u32 - 1) {
            Some(&self.vec[idx as usize])
        }
        else {
            None
        }
    }

    pub fn get_count(&self, name: &str) -> Option<u32> {
        if (self.tag_set.contains_key(name)) {
            Some(self.tag_set[name].count)
        }
        else {
            None
        }
    }

    pub fn get_count_idx(& self, idx: u32) -> Option<u32> {
        let name = self.get_name(idx);
        match name {
            Some(name) => {
                let count = self.get_count(name);
                Some(count.unwrap())
            }
            None => None
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
    pub fn from_triplets(values: HashMap<(usize, usize), f32>, n_rows: usize, n_cols: usize) -> CSR {
        let mut rows: Vec<Vec<(usize, f32)>> = vec![vec![]; n_rows];
        for (&(r, c), &v) in &values {
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
            val
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
        (self.n_rows + 1) * size_of::<usize>() +
            self.n_nz * size_of::<usize>() +
            self.n_nz * size_of::<f32>() +
            size_of::<Vec<f32>>() + size_of::<Vec<usize>>() * 2 +
            size_of::<f32>() + size_of::<usize>() * 2
    }
}

fn read_csv(path: &str) -> Result<(Tags, CSR), Box<dyn Error>> {
    use serde::Deserialize;
    #[derive(Debug,Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;

    let mut tags = Tags::new();
    let mut co_counts: HashMap<(usize, usize), f32> = HashMap::new();

    let mut i = 0;
    for line in reader.deserialize() {
        let row: Row = line?;
        let post_tags: Vec<&str> = row.tag_string
            .split_whitespace()
            .collect();
        for tag in post_tags.iter() {
            tags.add_or_increment(tag);
        }
        let post_tags_idxs: Vec<u32> = post_tags.iter()
            .map(|&s| tags.get_idx(s).unwrap())
            .collect();
        let combinations: Vec<(&u32, &u32)> = post_tags_idxs.iter()
            .cartesian_product(post_tags_idxs.iter())
            .filter(|(a, b)| a != b)
            .collect();
        for combo in combinations {
            let lhs = *combo.0 as usize;
            let rhs = *combo.1 as usize;
            *co_counts.entry((lhs, rhs)).or_insert(0.0) += 1.0;
        }
        i += 1;
    }

    println!("mammal: {:#?}", tags.get_idx("mammal").unwrap());
    println!("cat: {:#?}", tags.get_idx("4_legged").unwrap());

    let co_count_matrix = CSR::from_triplets(co_counts, tags.len(), tags.len());

    Ok( (tags, co_count_matrix) )
}

fn main() {
    let result = read_csv("lib/posts_egs.csv");
    match result {
        Err(e) => println!("Error: {:?}", e),
        Ok((_, co_count_matrix)) => {
            println!("{:?}", co_count_matrix.value(2, 3));
            println!("{:?}", co_count_matrix.n_nz);
            println!("{:?}", co_count_matrix.size());
        }
    }

}
