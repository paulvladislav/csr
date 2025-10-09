use crate::CSR;
use std::error::Error;

use std::ops::Add;

impl Add for &CSR {
    type Output = Result<CSR, Box<dyn Error>>;

    fn add(self, other: &CSR) -> Self::Output {
        if self.n_rows != other.n_rows || self.n_cols != other.n_cols {
            return Err("Matrices are not the same size".into());
        }

        let mut row_ptr = Vec::with_capacity(other.n_rows + 1);
        let mut col_idx = Vec::new();
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
                } else if rhs_i == rhs_row_end {
                    col_idx.push(self.col_idx[lhs_i]);
                    val.push(self.val[lhs_i]);
                    lhs_i += 1;
                } else {
                    let lhs_col = self.col_idx[lhs_i];
                    let rhs_col = other.col_idx[rhs_i];

                    if lhs_col == rhs_col {
                        let sum = self.val[lhs_i] + other.val[rhs_i];
                        if sum != 0.0 {
                            val.push(self.val[lhs_i] + other.val[rhs_i]);
                            col_idx.push(lhs_col);
                            lhs_i += 1;
                            rhs_i += 1;
                        }
                    } else if lhs_col < rhs_col {
                        val.push(self.val[lhs_i]);
                        col_idx.push(lhs_col);
                        lhs_i += 1;
                    } else if lhs_col > rhs_col {
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
            val,
        })
    }
}
