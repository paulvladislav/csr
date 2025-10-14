use csv::ReaderBuilder;
use std::error::Error;
use std::string::String;
use std::time::Instant;
use rand::prelude::*;
use rand::distr::weighted;

use csr_matrix::CSR;
use rand::distr::weighted::WeightedIndex;
use rand::{rng, thread_rng};
use crate::nmpi::{get_npmi_matrix, get_co_count_matrix, get_most_related_tags, get_npmi};
use crate::read_write::{write_data, read_posts, read_data};

mod nmpi;
mod read_write;
mod tags;



fn main() {
    let mut start = Instant::now();
    // print!("Reading posts csv...", );
    // let (n_posts, tags, posts_tags_idxs) = read_posts("data/posts.csv").unwrap();
    // println!("\t\t\t\t[Done in {:?}]", start.elapsed());
    //
    // start = Instant::now();
    // print!("Calculating co count matrix...");
    // let co_counts_matrix = get_co_count_matrix(n_posts, &tags, posts_tags_idxs).unwrap();
    // println!("\t\t\t[Done in {:?}]", start.elapsed());
    //
    //
    // start = Instant::now();
    // print!("Calculating npmi matrix...");
    // let npmi_matrix = get_npmi_matrix(n_posts, &tags, &co_counts_matrix);
    // println!("\t\t\t[Done in {:?}]", start.elapsed());
    //
    // start = Instant::now();
    // print!("Writing data...");
    // write_data("data/npmi", n_posts, &tags, &npmi_matrix);
    // println!("\t\t\t\t\t[Done in {:?}]", start.elapsed());

    start = Instant::now();
    print!("Reading data...");
    let (n_posts, tags, npmi_matrix) = read_data("data/npmi").unwrap();
    println!("\t\t\t\t\t[Done in {:?}]", start.elapsed());

    // let mut prompt_tag: Vec<&str> = vec!["loimu"];
    // let related: Vec<(&str, f32)> = get_most_related_tags(10, &npmi_matrix, vec![tags.get_idx("loimu").unwrap() as u32])
    //     .iter().map(|(i, s)| (tags.get_name(*i as usize).unwrap(), *s)).collect();

    let npmi = get_npmi("loimu", "deer", n_posts as u32, &npmi_matrix, &tags);

    println!("{:?}", npmi);
    // for _ in 0..10 {
    //     let idxs = prompt_tag.iter()
    //         .map(|t| tags.get_idx(t).unwrap() as u32)
    //         .collect();
    //     let tag_scores = get_most_related_tags(10, &npmi_matrix, idxs);
    //     let mut tag_idsx: Vec<u32> = Vec::new();
    //     let mut weights: Vec<f32> = Vec::new();
    //     for (idx, w) in tag_scores {
    //         tag_idsx.push(idx);
    //         weights.push(w);
    //     }
    //     let dist = WeightedIndex::new(weights).unwrap();
    //     let mut rng = rng();
    //     prompt_tag.push(tags.get_name(tag_idsx[dist.sample(&mut rng)] as usize).unwrap());
    //     println!("{:?}", prompt_tag);
    // }
}
