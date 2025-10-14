use std::arch::x86_64::_mm256_min_pd;
use std::cmp::Ordering::Greater;
use csr_matrix::CSR;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::thread;
use bincode::decode_from_reader;
use itertools::Itertools;
use crate::tags::Tags;

pub fn   get_co_count_matrix(
    n_posts: usize,
    tags: &Tags,
    posts_tag_idxs: Vec<Vec<u32>>,
) -> Result<(CSR), Box<dyn Error>> {
    const N_CHUNCK: usize = 12;

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
        co_count_matrix.add_in_place(&slice)?;
    }

    Ok(co_count_matrix)
}

pub fn get_npmi(tag_a: &str, tag_b: &str, n_posts: u32, co_cout_matrix: &CSR, tags: &Tags) -> f32 {
    let p_a  = tags.get_count(tag_a).unwrap() as f32 / n_posts as f32;
    let p_b  = tags.get_count(tag_b).unwrap() as f32 / n_posts as f32;
    let p_ab = co_cout_matrix.value(
            tags.get_idx(tag_a).unwrap(),
            tags.get_idx(tag_b).unwrap()
        )
        .unwrap()
        / n_posts as f32;



    (p_ab / (p_a * p_b)).log2() / -p_ab.log2()
}

pub fn get_npmi_matrix(
    n_posts: usize, 
    tags: &Tags, 
    co_count_matrix: &CSR
) -> CSR {
    let mut npmi_triples: Vec<(usize, usize, f32)> = Vec::with_capacity(co_count_matrix.n_nz);
    let post_freq = 1.0 / n_posts as f32;
    
    let mut probabilities = Vec::with_capacity(tags.len());
    for i in 0..tags.len() {
        let p = tags.get_count_idx(i)
            .unwrap()
            as f32 * post_freq;
        probabilities.push(p);
    }
    
    for (row, col, val) in co_count_matrix {
        let p_x = probabilities[row];
        let p_y = probabilities[col];
        let p_xy = val  * post_freq;
        let npmi_xy = (p_xy.log2() / (p_x.log2() * p_y.log2())) / -p_xy.log2();
        if npmi_xy > 0.0 {
            npmi_triples.push((row, col, npmi_xy));
        }
    }
    
    CSR::from_triples(&npmi_triples, tags.len(), tags.len())
}

pub fn get_most_related_tags(
    n_tags: usize,
    npmi_matrix: &CSR,
    tag_idxs: Vec<u32>
    ) -> Vec<(u32, f32)> {
    let mut scores: Vec<f32> = vec![0.0; npmi_matrix.n_cols];
    let mut counts: Vec<u32> = vec![0; npmi_matrix.n_cols];

    for tag_idx in &tag_idxs {
        for (rel_tag_idx, rel_score) in npmi_matrix.inter_row(*tag_idx as usize) {
            if !tag_idxs.contains(&(rel_tag_idx as u32)) {
                counts[rel_tag_idx] += 1;
                scores[rel_tag_idx] += rel_score;
            }
        }
    }

    for i in 0..npmi_matrix.n_cols {
        if counts[i] != 0 {
        scores[i] /= counts[i] as f32;
        }
    }

    scores.into_iter()
        .enumerate()
        .filter(|(i ,s)| *s > 0.0f32)
        .sorted_by(|a, b| {
            let s_a = a.1;
            let s_b = b.1;
            s_a.total_cmp(&s_b)
        })
        .rev()
        .map(|(i, s)| (i as u32, s))
        .take(n_tags)
        .collect()
}
