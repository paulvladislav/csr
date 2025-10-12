
use csr_matrix::CSR;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::thread;

use crate::tags::Tags;

pub fn get_co_counts(
    n_posts: usize,
    tags: &Tags,
    posts_tag_idxs: Vec<Vec<u32>>,
) -> Result<(CSR), Box<dyn Error>> {
    const N_CHUNCK: usize = 12;

    println!("Read {} posts", n_posts);
    println!("Read {} tags", tags.len());
    // println!("mammal: {:#?}", tags.get_idx("mammal").unwrap());
    // println!("domestic cat: {:#?}", tags.get_idx("domestic_cat").unwrap());

    let n_tags = tags.len();
    let mut handles = Vec::new();
    for chunk in posts_tag_idxs.chunks((n_posts / N_CHUNCK) + 1) {
        let chunk = chunk.to_vec();
        let handle = thread::spawn(move || {
            let mut co_counts: FxHashMap<(usize, usize), f32> = FxHashMap::default();

            for post_tag_idx in chunk {
                for i in 0..post_tag_idx.len() {
                    for j in (i + 1)..post_tag_idx.len() {
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
        println!("summing slice {} of {N_CHUNCK}", i + 1);
        co_count_matrix.add_in_place(&slice)?;
    }

    Ok(co_count_matrix)
}

pub fn calculate_npmi(n_posts: usize, tags: &Tags, co_count_matrix: &CSR) -> CSR {
    let mut npmi_triple: Vec<(usize, usize, f32)> = Vec::new();
    let post_freq = 1.0 / n_posts as f32;
    for (row, col, val) in co_count_matrix {
        let p_x = tags.get_count_idx(row as u32).unwrap() as f32 * post_freq;
        let p_y = tags.get_count_idx(row as u32).unwrap() as f32 * post_freq;
        let p_xy = val as f32 * post_freq;
        let npmi_xy = (p_xy.log2() - p_x.log2() - p_y.log2()) / -p_xy.log2();
        if npmi_xy > 0.0 {
            npmi_triple.push((row, col, npmi_xy));
        }
    }
    let npmi_matrix = CSR::from_triplet(&npmi_triple, tags.len(), tags.len());
    npmi_matrix
}
