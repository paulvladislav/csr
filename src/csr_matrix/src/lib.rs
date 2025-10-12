pub mod iter;
pub mod ops;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CSR {
    pub n_rows: usize,
    pub n_cols: usize,
    pub n_nz: usize,
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

    pub fn from_triplet(triplets: &Vec<(usize, usize, f32)>, n_rows: usize, n_cols: usize) -> CSR {
        let mut rows: Vec<Vec<(usize, f32)>> = vec![vec![]; n_rows];
        for (r, c, v) in triplets {
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
        todo!()
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
        let res = (&*self + other)?;
        self.n_nz = res.n_nz;
        self.row_ptr = res.row_ptr;
        self.col_idx = res.col_idx;
        self.val = res.val;

        Ok(())
    }
}
