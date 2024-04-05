use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::distributions::{Distribution, Uniform};
use base::matrix_vector_multiply::*;
use hydroflow_base::matrix_vector_multiply as hf;

fn rand_uniform_rows(num_rows: usize, num_dimensions:  usize, seed: Option<u64>) -> Vec<Vec<f64>> {
    // Create a random number generator with a fixed seed
    let seed = seed.unwrap_or(42);
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    // Generate random points
    let uniform = Uniform::new(-1.0, 1.0);
    let rows: Vec<Vec<f64>> = (0..num_rows)
        .map(|_| (0..num_dimensions).map(|_| uniform.sample(&mut rng)).collect::<Vec<f64>>())
        .collect();

    rows
}

fn transpose_rows_to_cols(rows: &Vec<Vec<f64>>) -> Vec<Vec<f64>> {
    let num_rows = rows.len();
    let num_cols = rows[0].len();
    let mut cols = vec![vec![0.0; num_rows]; num_cols];
    for i in 0..num_rows {
        for j in 0..num_cols {
            cols[j][i] = rows[i][j];
        }
    }
    cols
}

fn to_fixed_array(rows: &Vec<Vec<f64>>) -> Vec<[f64; 100]> {
    rows.iter().map(|row| {
        let mut arr = [0.0; 100];
        arr.iter_mut().zip(row.iter()).for_each(|(a, b)| *a = *b);
        arr
    }).collect()
}

fn matrix_vector_benchmark(c: &mut Criterion) {
    let dimensions = 100;
    let num_rows = 1000;

    let y = rand_uniform_rows(1, dimensions, None);
    let rows = rand_uniform_rows(num_rows, dimensions, None);
    let cols = transpose_rows_to_cols(&rows);

    c.bench_function("matrix_vector_multiply_row_major_loop", |b| {
        b.iter_batched(|| (rows.clone(), y[0].clone()), |(rows, y)| matrix_vector_multiply_row_major_loop(black_box(&rows), black_box(&y)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("matrix_vector_multiply_row_major_iterators", |b| {
        //b.iter(|| matrix_vector_multiply_row_major_iterators(black_box(&rows), black_box(&y[0])))
        b.iter_batched(|| (rows.clone(), y[0].clone()), |(rows, y)| matrix_vector_multiply_row_major_iterators(black_box(&rows), black_box(&y)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("matrix_vector_multiply_column_major_iterators", |b| {
        b.iter_batched(|| (cols.clone(), y.clone()), |(cols, y)| matrix_vector_multiply_column_major_iterators(black_box(&cols), black_box(&y[0])), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::matrix_vector_multiply_column_major_iterators_nested", |b| {
        b.iter_batched(|| (cols.clone(), y[0].clone()), |(cols, y)| hf::matrix_vector_multiply_column_major_iterators_nested(black_box(cols), black_box(y)), criterion::BatchSize::SmallInput)
    });
    
    c.bench_function("hf::matrix_vector_multiply_column_major_iterators_unpacked_index", |b| {
        b.iter_batched(|| (cols.clone(), y[0].clone()), |(cols, y)| hf::matrix_vector_multiply_column_major_iterators_unpacked_index(black_box(cols), black_box(y)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("matrix_vector_multiply_column_major_loop", |b| {
        b.iter_batched(|| (cols.clone(), y.clone()), |(cols, y)| matrix_vector_multiply_column_major_loop(black_box(&cols), black_box(&y[0])), criterion::BatchSize::SmallInput)
    });
    
    c.bench_function("hf::matrix_vector_multiply_column_major_iterators_unpacked_zip", |b| {
        b.iter_batched(|| (cols.clone(), y[0].clone()), |(cols, y)| hf::matrix_vector_multiply_column_major_iterators_unpacked_zip(black_box(cols), black_box(y)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("matrix_vector_multiply_column_major_loop_fixed", |b| {
        let cols = to_fixed_array(&cols);
        let y = to_fixed_array(&y);

        b.iter_batched(|| (cols.clone(), y.clone()), |(cols, y)| matrix_vector_multiply_column_major_loop_fixed(black_box(&cols), black_box(y[0])), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::matrix_vector_multiply_column_major_iterators_unpacked_zip_fixed", |b| {
        let cols = to_fixed_array(&cols);
        let y = to_fixed_array(&y);
        b.iter_batched(|| (cols.clone(), y[0].clone()), |(cols, y)| hf::matrix_vector_multiply_column_major_iterators_unpacked_zip_fixed(black_box(cols), black_box(y)), criterion::BatchSize::SmallInput)
    });
}

criterion_group!{
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(8));
    targets = matrix_vector_benchmark
}
criterion_main!(benches);