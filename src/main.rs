use csv::ReaderBuilder;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::rc::Rc;
use std::string::String;
use std::time::Instant;
use std::thread;
use itertools::Itertools;
use std::ops::Add;

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
    pub fn new(n_rows: usize, n_cols: usize) -> CSR {
        CSR {
            n_rows,
            n_cols,
            n_nz: 0,
            row_ptr: vec![0; n_rows + 1],
            col_idx: Vec::new(),
            val: Vec::new(),
        }
    }

    pub fn from_triplet(triplet: &Vec<(usize, usize, f32)>, n_rows: usize, n_cols: usize) -> CSR {
        let mut rows: Vec<Vec<(usize, f32)>> = vec![vec![]; n_rows];
        for (r, c, v) in triplet {
            rows[*r].push((*c, *v));
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
            if row.is_empty() {
                row_ptr.push(*row_ptr.last().unwrap());
                continue;
            }
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

    pub fn from_fxhash(
        fxhash: &FxHashMap<(usize, usize), f32>,
        n_rows: usize,
        n_cols: usize,
    ) -> CSR {
        let mut rows: Vec<Vec<(usize, f32)>> = vec![vec![]; n_rows];
        for (&(r, c), &v) in fxhash {
            rows[r].push((c, v));
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
            if row.is_empty() {
                row_ptr.push(*row_ptr.last().unwrap());
                continue;
            }
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
        if row > self.n_rows || col > self.n_cols {
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

    pub fn insert(&mut self, row: usize, col: usize, val: f32) {
        if row > self.n_rows || col > self.n_cols {
            return;
        }
        let row_start = self.row_ptr[row];
        let row_end = self.row_ptr[row + 1];
        if row_start == row_end {
            self.row_ptr[row] = row;
        }
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

    pub fn add_in_place(&mut self, other: &CSR) -> Result<(), Box<dyn Error>> {
        let res = (& *self + other)?;
        self.n_nz = res.n_nz;
        self.row_ptr = res.row_ptr;
        self.col_idx = res.col_idx;
        self.val = res.val;

        Ok(())
    }
}

impl Add for &CSR {
    type Output = Result<CSR, Box<dyn Error>>;

    fn add(self, other: &CSR) -> Self::Output {
        if self.n_rows != other.n_rows || self.n_cols != other.n_cols {
            return Err("Matrices are not the same size".into());
        }

        let mut row_ptr = Vec::with_capacity(other.n_rows + 1);
        let mut col_idx= Vec::new();
        let mut val = Vec::new();

        row_ptr.push(0);
        for i in 0..self.n_rows {
            let lhs_row_start = self.row_ptr[i];
            let lhs_row_end = self.row_ptr[i + 1];
            let rhs_row_start = other.row_ptr[i];
            let rhs_row_end = other.row_ptr[i + 1];

            let mut lhs_i = lhs_row_start;
            let mut rhs_i = rhs_row_start;
            while lhs_i < lhs_row_end || rhs_i < rhs_row_end {
                if lhs_i == lhs_row_end {
                    col_idx.push(other.col_idx[rhs_i]);
                    val.push(other.val[rhs_i]);
                    rhs_i += 1;
                }
                else if rhs_i == rhs_row_end {
                    col_idx.push(self.col_idx[lhs_i]);
                    val.push(self.val[lhs_i]);
                    lhs_i += 1;
                }
                else {
                    let lhs_col = self.col_idx[lhs_i];
                    let rhs_col = other.col_idx[rhs_i];

                    if lhs_col == rhs_col {
                        val.push(self.val[lhs_i] + other.val[rhs_i]);
                        col_idx.push(lhs_col);
                        lhs_i += 1;
                        rhs_i += 1;
                    }
                    else if lhs_col < rhs_col {
                        val.push(self.val[lhs_i]);
                        col_idx.push(lhs_col);
                        lhs_i += 1;
                    }
                    else if lhs_col > rhs_col {
                        val.push(other.val[rhs_i]);
                        col_idx.push(rhs_col);
                        rhs_i += 1;
                    }

                }
            }
            row_ptr.push(val.len());
        }

        Ok(CSR {
            n_rows: self.n_rows,
            n_cols: self.n_cols,
            n_nz: val.len(),
            row_ptr,
            col_idx,
            val
        })
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
    // let mut co_counts: FxHashMap<(usize, usize), f32> = FxHashMap::default();

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

    let n_tags = tags.len();
    let mut handles = Vec::new();
    for chunk in posts_tag_idxs.chunks((n_posts / 8) + 1) {
        let chunk = chunk.to_vec();
        let handle = thread::spawn( move || {
            let mut co_counts: FxHashMap<(usize, usize), f32> = FxHashMap::default();

            for post_tag_idx in chunk {

                for i in 0..post_tag_idx.len() {
                    for j in (i+1)..post_tag_idx.len() {
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
        co_count_matrix.add_in_place(&slice)?;
    }


    let elapsed = read_start.elapsed();
    println!("reading took {:?}", elapsed);

    println!("mammal: {:#?}", tags.get_idx("mammal").unwrap());
    println!("cat: {:#?}", tags.get_idx("cat").unwrap());

    Ok((tags, co_count_matrix))
}

fn main() {
    let result = read_csv("lib/posts.csv");
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
