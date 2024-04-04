pub fn matrix_vector_multiply_row_major_loop(matrix_rows: &[Vec<f64>], vector: &[f64]) -> Vec<f64> {
    let m = matrix_rows.len();
    let n = matrix_rows[0].len();
    let mut result = vec![0.0; m];

    for i in 0..m {
        for j in 0..n {
            result[i] += matrix_rows[i][j] * vector[j];
        }
    }

    result
}

pub fn matrix_vector_multiply_column_major_loop(matrix_columns: &[Vec<f64>], vector: &[f64]) -> Vec<f64> {
    let m = matrix_columns[0].len();
    let n = matrix_columns.len();
    let mut result = vec![0.0; m];

    for i in 0..m {
        for j in 0..n {
            result[i] += matrix_columns[j][i] * vector[j];
        }
    }

    result
}

pub fn matrix_vector_multiply_row_major_iterators(matrix_rows: &[Vec<f64>], vector: &[f64]) -> Vec<f64> {
    matrix_rows.iter().map(|row| row.iter().zip(vector.iter()).map(|(a, b)| a * b).sum()).collect()
}

pub fn matrix_vector_multiply_column_major_iterators(matrix_columns: &[Vec<f64>], vector: &[f64]) -> Vec<f64> {
    let res = matrix_columns.iter()
        .zip(vector.iter())
        .map(|(column, y_j)| {
            let v = column.iter().map(|x_ij| x_ij * y_j)
                .collect::<Vec<f64>>();
            v
        })
        .reduce(|acc, x| {
            acc.iter().zip(x.iter()).map(|(a, b)| a + b).collect()
        });
    return res.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matrix_vector_multiply_row_major_loop() {
        let matrix_rows = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_row_major_loop(&matrix_rows, &vector),
            expected_result
        );
    }

    #[test]
    fn test_matrix_vector_multiply_column_major_loop() {
        let matrix_columns = vec![
            vec![1.0, 4.0, 7.0],
            vec![2.0, 5.0, 8.0],
            vec![3.0, 6.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_column_major_loop(&matrix_columns, &vector),
            expected_result
        );
    }

    #[test]
    fn test_matrix_vector_multiply_row_major_iterators() {
        let matrix_rows = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_row_major_iterators(&matrix_rows, &vector),
            expected_result
        );
    }

    #[test]
    fn test_matrix_vector_multiply_column_major_iterators() {
        let matrix_columns = vec![
            vec![1.0, 4.0, 7.0],
            vec![2.0, 5.0, 8.0],
            vec![3.0, 6.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_column_major_iterators(&matrix_columns, &vector),
            expected_result
        );
    }
}
