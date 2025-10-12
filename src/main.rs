use csv::ReaderBuilder;
use std::error::Error;
use std::string::String;
use std::time::Instant;

use csr_matrix::CSR;
use crate::nmpi::{calculate_npmi, get_co_counts};
use crate::read_write::{write_data, read_posts, read_data};

mod nmpi;
mod read_write;
mod tags;



fn main() {

    let mut start = Instant::now();
    print!("Reading posts csv...", );
    let (n_posts, tags, posts_tags_idxs) = read_posts("data/posts.csv").unwrap();
    println!("\t\t\t\t[Done in {:?}]", start.elapsed());

    start = Instant::now();
    print!("Calculating co count matrix...");
    let co_counts_matrix = get_co_counts(n_posts, &tags, posts_tags_idxs).unwrap();
    println!("\t\t\t[Done in {:?}]", start.elapsed());

    start = Instant::now();
    print!("Calculating npmi matrix...");
    let npmi_matrix = calculate_npmi(n_posts, &tags, &co_counts_matrix);
    println!("\t\t\t[Done in {:?}]", start.elapsed());

    start = Instant::now();
    print!("Writing data...");
    write_data("data/npmi", n_posts, &tags, &npmi_matrix);
    println!("\t\t\t\t\t[Done in {:?}]", start.elapsed());
}
