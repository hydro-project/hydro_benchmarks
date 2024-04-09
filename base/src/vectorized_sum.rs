pub fn vectorized_sum_iterator(a: Vec<f32>) -> f32 {
    a.iter().sum()
}

pub fn vectorized_sum_iterator_chunks<const CHUNK_SIZE: usize>(a: Vec<f32>) -> f32 {
    a.chunks(CHUNK_SIZE).map(|chunk| chunk.iter().sum::<f32>()).sum()
}

pub fn vectorized_sum_iterator_batching<const CHUNK_SIZE: usize>(a: Vec<f32>) -> f32 {
    
    let mut buf: [f32; CHUNK_SIZE] = [0.0; CHUNK_SIZE];
    a.into_iter().enumerate()
        .map(move |(i,f)|{
            buf[i%CHUNK_SIZE] = f;
            if i%CHUNK_SIZE == CHUNK_SIZE-1 {
                Some(buf.clone())
            } else {
                None
            }
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .map(|chunk| chunk.iter().sum::<f32>())
        .sum()
}

pub fn vectorized_sum_iterator_batched<const CHUNK_SIZE: usize>(a: Vec<[f32; CHUNK_SIZE]>) -> f32 {
    
    a.into_iter().map(|chunk| chunk.iter().sum::<f32>()).sum()
}

pub fn vectorized_sum_iterator_batched_flatten<const CHUNK_SIZE: usize>(a: Vec<[f32; CHUNK_SIZE]>) -> f32 {
    
    a.into_iter().flatten().sum()
}

pub fn to_chunks<const CHUNK_SIZE: usize>(a: &[f32]) -> Vec<[f32; CHUNK_SIZE]> {
    a.chunks(CHUNK_SIZE).map(|chunk| {
        let mut arr = [0.0; CHUNK_SIZE];
        for (i, &val) in chunk.iter().enumerate() {
            arr[i] = val;
        }
        arr
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vectorized_sum_iterator() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = vectorized_sum_iterator(a);
        assert_eq!(result, 15.0);
    }

    #[test]
    fn test_vectorized_sum_iterator_chunks() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = vectorized_sum_iterator_chunks::<2>(a);
        assert_eq!(result, 15.0);
    }

    #[test]
    fn test_vectorized_sum_iterator_batched() {
        let a = vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]];
        let result = vectorized_sum_iterator_batched::<2>(a);
        assert_eq!(result, 21.0);
    }

    #[test]
    fn test_vectorized_sum_iterator_batched_flatten() {
        let a = vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]];
        let result = vectorized_sum_iterator_batched_flatten::<2>(a);
        assert_eq!(result, 21.0);
    }

    #[test]
    fn test_to_chunks() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = to_chunks::<2>(&a);
        assert_eq!(result, vec![[1.0, 2.0], [3.0, 4.0], [5.0, 0.0]]);
    }
}
