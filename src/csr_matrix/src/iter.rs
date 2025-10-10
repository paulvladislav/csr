use crate::CSR;

pub struct CSRIter<'a> {
    csr: &'a CSR,
    row: usize,
    idx: usize,
}

impl CSR {
    pub fn iter(&self) -> CSRIter {
        CSRIter {
            csr: self,
            row: 0,
            idx: 0,
        }
    }
}

impl<'a> Iterator for CSRIter<'a> {
    type Item = (usize, usize, f32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.csr.n_nz {
            return None;
        }

        while self.idx >= self.csr.row_ptr[self.row + 1] {
            self.row += 1;
        }

        let row = self.row;
        let col = self.csr.col_idx[self.idx];
        let val = self.csr.val[self.idx];
        self.idx += 1;

        Some((row, col, val))
    }
}

impl<'a> IntoIterator for &'a CSR {
    type Item = (usize, usize, f32);
    type IntoIter = CSRIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
