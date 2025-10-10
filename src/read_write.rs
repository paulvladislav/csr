use crate::*;

pub fn read_posts(path: &str) -> Result<(usize, Tags, Vec<Vec<u32>>), Box<dyn Error>> {
    use serde::Deserialize;
    #[derive(Debug, Deserialize)]
    struct Row {
        tag_string: String,
    }

    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;

    let mut tags = Tags::new();

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

    Ok((n_posts, tags, posts_tag_idxs))
}
