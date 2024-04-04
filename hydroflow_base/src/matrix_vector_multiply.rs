use hydroflow::hydroflow_syntax;

pub fn matrix_vector_multiply_column_major_iterators_nested(columns: Vec<Vec<f64>>, vector: Vec<f64>) -> Vec<f64> {
    
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<Vec<f64>>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        columns = source_iter(columns);
        vector = source_iter(vector);

        // Combine columns and vector elements
        columns_vector = zip();
        columns -> [0]columns_vector;
        vector -> [1]columns_vector;

        result = columns_vector
            -> map(|(column, y_j)| {
                let v = column.iter().map(|x_ij| {
                    x_ij * y_j
                })
                    .collect::<Vec<f64>>();
                v
            }) 
            -> reduce(|acc: &mut Vec<f64>, x: Vec<f64>| {
                for (a, b) in acc.iter_mut().zip(x.iter()) {
                    *a += b;
                }
        });

        result
            //-> inspect(|x| println!("Result: {:?}", x))
            -> for_each(|x| output_send.send(x).unwrap());

    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<Vec<f64>>,_>(output_recv);

    assert!(res.len() == 1);

    return res[0].clone();
}

pub fn matrix_vector_multiply_column_major_iterators_unpacked_index(columns: Vec<Vec<f64>>, vector: Vec<f64>) -> Vec<f64> {
    
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<Vec<f64>>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        columns = source_iter(columns) -> enumerate() -> flat_map(|(j, column)| {
            column.into_iter().enumerate().map(move |(i, x_ij)| (j, (i, x_ij)))
        });
        vector = source_iter(vector) -> enumerate();

        // Combine columns and vector elements
        columns_vector = join_multiset();
        columns -> [0]columns_vector;
        vector -> [1]columns_vector;

        result = columns_vector
            -> map(|(_j, ((i, x_ij), y_j))| (i, (x_ij, y_j)))
            -> fold_keyed(|| 0.0, |acc: &mut f64, (column, y_j)| {
                *acc += column * y_j;
            })
            //-> inspect(|x| println!("Result: {:?}", x))
            //-> sort_by_key(|(i, _)| i)
            -> map(|(_, x)| x)
            -> fold(Vec::<f64>::new, |acc, x| {
                acc.push(x);
            });

        result
            -> for_each(|x| output_send.send(x).unwrap());

    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<Vec<f64>>,_>(output_recv);

    assert!(res.len() == 1);

    return res[0].clone();
}

pub fn matrix_vector_multiply_column_major_iterators_unpacked_zip(columns: Vec<Vec<f64>>, vector: Vec<f64>) -> Vec<f64> {

    //let n = columns.len();
    let m = columns[0].len();
    
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<Vec<f64>>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        columns = source_iter(columns);
        vector = source_iter(vector);

        // Combine columns and vector elements
        columns -> [0]columns_vector;
        vector -> [1]columns_vector;
        columns_vector = zip() -> flat_map(|(column, y_j)| {
            column.into_iter()
                .map(move |x_ij| (x_ij, y_j))
        });
        
        result = columns_vector
            //-> inspect(|x| println!("Columns vector: {:?}", x))
            -> map(|(x_ij, y_j)| x_ij * y_j)
            -> enumerate()
            -> map(|(index_flat, x)| {
                // Recompute the i and j index for x from the flat index
                let i = index_flat % m;
                (i, x)
            })
            -> reduce_keyed(|acc: &mut f64, x| {
                *acc += x;
            })
            //-> inspect(|x| println!("Result: {:?}", x))
            -> sort_by_key(|(i, _)| i)
            -> map(|(_, x)| x)
            -> fold(Vec::<f64>::new, |acc, x| {
                acc.push(x);
            });

        result
            -> for_each(|x| output_send.send(x).unwrap());

    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<Vec<f64>>,_>(output_recv);

    assert!(res.len() == 1);

    return res[0].clone();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matrix_vector_multiply_column_major_iterators_nested() {
        let matrix_columns = vec![
            vec![1.0, 4.0, 7.0],
            vec![2.0, 5.0, 8.0],
            vec![3.0, 6.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_column_major_iterators_nested(matrix_columns, vector),
            expected_result
        );
    }
    
    #[test]
    fn test_matrix_vector_multiply_column_major_iterators_unpacked() {
        let matrix_columns = vec![
            vec![1.0, 4.0, 7.0],
            vec![2.0, 5.0, 8.0],
            vec![3.0, 6.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_column_major_iterators_unpacked_index(matrix_columns, vector),
            expected_result
        );
    }

    #[test]
    fn test_matrix_vector_multiply_column_major_iterators_unpacked_zip() {
        let matrix_columns = vec![
            vec![1.0, 4.0, 7.0],
            vec![2.0, 5.0, 8.0],
            vec![3.0, 6.0, 9.0],
        ];
        let vector = vec![1.0, 2.0, 3.0];
        let expected_result = vec![14.0, 32.0, 50.0];
        assert_eq!(
            matrix_vector_multiply_column_major_iterators_unpacked_zip(matrix_columns, vector),
            expected_result
        );
    }
}