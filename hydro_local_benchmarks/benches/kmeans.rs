use base::kmeans_baseline::kmeans;
use hydroflow_base::kmeans_hf::kmeans_hf;
use base::point::Point;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::distributions::{Distribution, Uniform};

fn rand_uniform_points(num_points: usize, num_dimensions:  usize, seed: Option<u64>) -> Vec<Point> {
    // Create a random number generator with a fixed seed
    let seed = seed.unwrap_or(42);
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    // Generate random points
    let uniform = Uniform::new(-1.0, 1.0);
    let points: Vec<Point> = (0..num_points)
        .map(|_| Point{coordinates:(0..num_dimensions).map(|_| uniform.sample(&mut rng)).collect()})
        .collect();

    points
}

fn kmeans_benchmark(c: &mut Criterion) {
    // Benchmark the kmeans function with fixed number of iterations for deterministic results
    // XXX: Also set this inside of kmeans_hf!
    let max_iterations = 100;
    let k = 2;
    let num_points = 1000;
    let num_dimensions = 2;
    let points = rand_uniform_points(num_points, num_dimensions, Some(42));
    let tolerance = 0.0;

    c.bench_function("kmeans_baseline", |b| {
        b.iter_batched(|| points.clone(), |points| kmeans(points, k, max_iterations, tolerance), criterion::BatchSize::SmallInput)
    });
    
    c.bench_function("kmeans_hydroflow", |b| {
        b.iter_batched(|| points.clone(), |points| kmeans_hf(points, k, max_iterations, tolerance), criterion::BatchSize::SmallInput)
    });
    
}

criterion_group!(benches, kmeans_benchmark);
criterion_main!(benches);