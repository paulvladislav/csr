mod nmpi;
mod read_write;
mod tags;

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
use serde::Serialize;
use crate::nmpi::{calculate_npmi, get_co_counts};
use crate::read_write::write_data;

fn main() {
    let (n_posts, tags, posts_tags_idxs) = read_write::read_posts("data/posts_egs.csv").unwrap();
    let co_counts_matrix = get_co_counts(n_posts, &tags, posts_tags_idxs). unwrap();
    let npmi_matrix = calculate_npmi(n_posts, &tags, &co_counts_matrix);
    println!("wrote {:?}", npmi_matrix);
    println!();
    write_data("data/npmi", n_posts, &tags, &npmi_matrix);

    let (n_posts, tags, npmi_matrix) = read_write::read_data("data/npmi").unwrap();
    println!("read {:?}", npmi_matrix);
}
