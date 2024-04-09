use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::distributions::{Distribution, Uniform};
use base::vectorized_sum::*;
use hydroflow_base::vectorized_sum as hf;

fn rand_uniform_vector(num_elem: usize, seed: Option<u64>) -> Vec<f32> {
    // Create a random number generator with a fixed seed
    let seed = seed.unwrap_or(42);
    let rng: StdRng = SeedableRng::seed_from_u64(seed);

    // Generate random points
    let uniform = Uniform::new(-1.0, 1.0);
    uniform.sample_iter(rng).take(num_elem).collect::<Vec<f32>>()
}

fn vectorized_sum_benchmark(c: &mut Criterion) {

    let num_elem = 10000;
    let a = rand_uniform_vector(num_elem, None);

    c.bench_function("vectorized_sum_iterator", |b| {
        b.iter_batched(|| a.clone(), |a| vectorized_sum_iterator(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_chunks-4", |b| {
        b.iter_batched(|| a.clone(), |a| vectorized_sum_iterator_chunks::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_chunks-8", |b| {
        b.iter_batched(|| a.clone(), |a| vectorized_sum_iterator_chunks::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batching-4", |b| {
        b.iter_batched(|| a.clone(), |a| vectorized_sum_iterator_batching::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batching-8", |b| {
        b.iter_batched(|| a.clone(), |a| vectorized_sum_iterator_batching::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batched-4", |b| {
        b.iter_batched(|| to_chunks(&a), |a| vectorized_sum_iterator_batched::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batched-8", |b| {
        b.iter_batched(|| to_chunks(&a), |a| vectorized_sum_iterator_batched::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batched_flatten-4", |b| {
        b.iter_batched(|| to_chunks(&a), |a| vectorized_sum_iterator_batched_flatten::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("vectorized_sum_iterator_batched_flatten-8", |b| {
        b.iter_batched(|| to_chunks(&a), |a| vectorized_sum_iterator_batched_flatten::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    // Hydroflow
    c.bench_function("hf::vectorized_sum_iterator", |b| {
        b.iter_batched(|| a.clone(), |a| hf::vectorized_sum_iterator(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_chunks-4", |b| {
        b.iter_batched(|| a.clone(), |a| hf::vectorized_sum_iterator_chunks::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_chunks-8", |b| {
        b.iter_batched(|| a.clone(), |a| hf::vectorized_sum_iterator_chunks::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_batching-4", |b| {
        b.iter_batched(|| a.clone(), |a| hf::vectorized_sum_iterator_batching::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_batching-8", |b| {
        b.iter_batched(|| a.clone(), |a| hf::vectorized_sum_iterator_batching::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_batched-4", |b| {
        b.iter_batched(|| to_chunks(&a), |a| hf::vectorized_sum_iterator_batched::<4>(black_box(a)), criterion::BatchSize::SmallInput)
    });

    c.bench_function("hf::vectorized_sum_iterator_batched-8", |b| {
        b.iter_batched(|| to_chunks(&a), |a| hf::vectorized_sum_iterator_batched::<8>(black_box(a)), criterion::BatchSize::SmallInput)
    });

}

criterion_group!(benches, vectorized_sum_benchmark);
criterion_main!(benches);