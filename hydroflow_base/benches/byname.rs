use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hydroflow_base::tpcc::byname::*;

#[derive(Clone, Copy)]
enum ByNameImplementation {
    HydroFlowSortFold,
    HydroFlowInlined,
    HydroFlowIncrementalSort,
    HydroFlowIncrementalSortWithDelete,
}

impl std::fmt::Display for ByNameImplementation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ByNameImplementation::HydroFlowSortFold => write!(f, "HydroFlowPlain"),
            ByNameImplementation::HydroFlowInlined => write!(f, "HydroFlowInlined"),
            ByNameImplementation::HydroFlowIncrementalSort => write!(f, "HydroFlowIncrementalSort"),
            ByNameImplementation::HydroFlowIncrementalSortWithDelete => write!(f, "HydroFlowIncrementalSortWithDelete"),
        }
    }
}

#[derive(Clone, Copy)]
struct ExperimentInput {
    base_size: i32,
    update_size: i32,
    delete_size: i32,
    implementation: ByNameImplementation,
}

impl std::fmt::Display for ExperimentInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} Base Size={}, Update Size={}, Delete Size={}",
            self.implementation, self.base_size, self.update_size, self.delete_size
        )
    }
}

fn byname(c: &mut criterion::Criterion) {
    
    let c_last = "lastname".to_string();
    let c_d_id = 43;
    let c_w_id = 44;

    let base_size = 3000; // Number of customers per district
                          //let update_sizes = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    let update_sizes = vec![0, 1, 2, 4, 8, 16];
    let implementations = vec![
        //ByNameImplementation::HydroFlowSortFold,
        //ByNameImplementation::HydroFlowInlined,
        ByNameImplementation::HydroFlowIncrementalSort,
        ByNameImplementation::HydroFlowIncrementalSortWithDelete,
    ];
    let inputs: Vec<_> = update_sizes
        .into_iter()
        .flat_map(|update_size| {
            implementations
                .iter()
                .map(move |&implementation| (update_size, implementation))
        })
        .map(|(update_size, implementation)| ExperimentInput {
            base_size,
            update_size,
            delete_size: update_size, // delete as many as we update
            implementation,
        })
        .collect();

    let mut group = c.benchmark_group("Byname");

    for input in inputs {
        group.throughput(criterion::Throughput::Elements(input.update_size as u64 * 2));
        group.bench_with_input(
            BenchmarkId::new(format!("{}", input.implementation), input),
            &input,
            |b, input| {
                let (base_vals, update_vals, delete_vals, _transaction_paramters) = generate_customers(input.base_size, input.update_size, input.delete_size);
                
                b.iter_batched(|| {
                    let combined_data = Vec::from_iter(base_vals.clone().into_iter().chain(update_vals.clone().into_iter()));

                    let (flow, flow_updates) = match input.implementation {
                        ByNameImplementation::HydroFlowSortFold => {
                            let flow = median_sort_fold(base_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = median_sort_fold(combined_data, c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                        ByNameImplementation::HydroFlowInlined => {
                            let flow = median_inlined(base_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = median_inlined(combined_data, c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                        ByNameImplementation::HydroFlowIncrementalSort => {
                            let flow = median_incremental_sort_inlined(base_vals.clone(), update_vals.clone(), delete_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = median_incremental_sort_inlined(base_vals.clone(), update_vals.clone(), delete_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                        ByNameImplementation::HydroFlowIncrementalSortWithDelete => {
                            let flow = median_incremental_sort_inlined_with_deletions(base_vals.clone(), update_vals.clone(), delete_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = median_incremental_sort_inlined_with_deletions(base_vals.clone(), update_vals.clone(), delete_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                    };

                    (flow, flow_updates)
                    
                }, |(mut flow, mut flow_updates)| {
                    flow.run_available();

                    if input.update_size > 0 {
                        match input.implementation {
                            ByNameImplementation::HydroFlowIncrementalSort => {flow.run_available();}
                            _ => {flow_updates.run_available();}
                        }
                    }
                }, criterion::BatchSize::SmallInput);
            });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(30));
    targets = byname
}
criterion_main!(benches);