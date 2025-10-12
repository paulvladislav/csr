use crate::*;
use crate::tags::Tags;

use bincode;
use serde::{Deserialize, Serialize};
use std::fs;
use bincode::config::Config;

#[derive(Serialize, Deserialize)]
struct SerializedData {
    n_posts: usize,
    tags: Box<Tags>,
    csr: Box<CSR>
}

pub fn read_posts(path: &str) -> Result<(usize, Tags, Vec<Vec<u32>>), Box<dyn Error>> {
    use serde::Deserialize;
    #[derive(Debug, Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;

    let mut tags = Tags::new();

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

    Ok((n_posts, tags, posts_tag_idxs))
}

pub fn write_data(path: &str, n_posts: usize, tags: &Tags, npmi_matrix: &CSR) {
    let cfg = bincode::config::standard();
    let s = SerializedData{
        n_posts,
        tags: Box::new(tags.clone()),
        csr: Box::new(npmi_matrix.clone())
    };
    let encoded = bincode::serde::encode_to_vec(s, cfg).unwrap();

    fs::write(path, encoded).unwrap();
}

pub fn read_data(path: &str) -> Result<(usize, Tags, CSR), Box<dyn Error>> {
    let data = fs::read(path)?;
    let cfg = bincode::config::standard();
    let (ser, _): (SerializedData, _) = bincode::serde::decode_from_slice(&data, cfg)?;

    let n_posts = ser.n_posts;
    let tags = *ser.tags;
    let npmi_matrix = *ser.csr;

    Ok((n_posts, tags, npmi_matrix))
}
