use base::tpch::{
    initialize::initialize_database, query_1::load as load_q1, query_19::load as load_q19,
    query_4::load as load_q4,
};
//use base::tpch::query_1::query as query_1_base;
use base::tpch::query_1::query_duckdb as query_1_duckdb;
use base::tpch::query_19::query_duckdb as query_19_duckdb;
use hydroflow_base::tpch::query_1::query as query_1_hf;
use hydroflow_base::tpch::query_1::query_base as query_1_base;
use hydroflow_base::tpch::query_19::query as query_19_hf;
use hydroflow_base::tpch::query_19::query_base as query_19_base;
//use base::tpch::query_4::query as query_4_base;
use base::tpch::query_4::query_duckdb as query_4_duckdb;
use criterion::{criterion_group, criterion_main, Criterion};
use hydroflow_base::tpch::query_4::query as query_4_hf;
use hydroflow_base::tpch::query_4::query_base as query_4_base;

/**
* Query 1 is a straight pipeline that is well suited for compiling.
* We expect HF to be faster than DuckDB.
*/
fn tpch_sf1_query_1(c: &mut Criterion) {
    let scale_factor = 1;
    let conn = initialize_database(scale_factor);

    c.bench_function("query_1_baseline", |b| {
        b.iter_batched(
            || load_q1(&conn),
            |line_items| query_1_base(line_items),
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("query_1_hf", |b| {
        b.iter_batched(
            || load_q1(&conn),
            |line_items| query_1_hf(line_items),
            criterion::BatchSize::SmallInput,
        )
    });

    // Set duckdb for benchmarking to single thread
    let _ = conn.execute("SET threads = 1;", []);
    c.bench_function("query_1_duckdb", |b| b.iter(|| query_1_duckdb(&conn, None)));
}

/**
 * Query 4 is medium complex. All implementations should be on par.
 */
fn tpch_sf1_query_4(c: &mut Criterion) {
    let scale_factor = 1;
    let conn = initialize_database(scale_factor);

    c.bench_function("query_4_baseline", |b| {
        b.iter_batched(
            || load_q4(&conn),
            |(line_items, orders)| query_4_base(line_items, orders),
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("query_4_hf", |b| {
        b.iter_batched(
            || load_q4(&conn),
            |(line_items, orders)| query_4_hf(line_items, orders),
            criterion::BatchSize::SmallInput,
        )
    });

    // Set duckdb for benchmarking to single thread
    let _ = conn.execute("SET threads = 1;", []);
    c.bench_function("query_4_duckdb", |b| b.iter(|| query_4_duckdb(&conn, None)));
}

/**
 * Query 19 is well suited for vectorization and should be faster with DuckDB.
 */
fn tpch_sf1_query_19(c: &mut Criterion) {
    let scale_factor = 1;
    let conn = initialize_database(scale_factor);

    c.bench_function("query_19_baseline", |b| {
        b.iter_batched(
            || load_q19(&conn),
            |(line_items, part)| query_19_base(line_items, part),
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("query_19_hf", |b| {
        b.iter_batched(
            || load_q19(&conn),
            |(line_items, part)| query_19_hf(line_items, part),
            criterion::BatchSize::SmallInput,
        )
    });

    // Set duckdb for benchmarking to single thread
    let _ = conn.execute("SET threads = 1;", []);
    c.bench_function("query_19_duckdb", |b| {
        b.iter(|| query_19_duckdb(&conn, None))
    });
}

criterion_group!(
    benches,
    tpch_sf1_query_1,
    tpch_sf1_query_4,
    tpch_sf1_query_19,
);
criterion_main!(benches);
