use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hydroflow_base::tpcc::byname::*;

#[derive(Clone, Copy, Debug)]
enum ByNameImplementation {
    HydroFlowSortFold,
    HydroFLowInlined
}

impl std::fmt::Display for ByNameImplementation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ByNameImplementation::HydroFlowSortFold => write!(f, "HydroFlowPlain"),
            ByNameImplementation::HydroFLowInlined => write!(f, "HydroFlowInlined"),
        }
    }
}

#[derive(Clone, Copy)]
struct ExperimentInput {
    base_size: i32,
    updates_size: i32,
    implementation: ByNameImplementation,
}

impl std::fmt::Display for ExperimentInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} Base Size={}, Updates Size={}",
            self.implementation, self.base_size, self.updates_size
        )
    }
}

fn generate_data(input: &ExperimentInput) -> (Vec<Customer>, Vec<Customer>, (Option<i32>, Option<i32>, Option<i32>, Option<String>)) {
    /*
                c_id INT,
                c_d_id INT,
                c_w_id INT,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                 */
                let c_d_id = 43;
                let c_w_id = 44;
                //let c_first = "first_name".to_string();
                let c_middle = "".to_string();
                let c_last = "lastname".to_string();

                let base_vals: Vec<Customer> = (0..input.base_size)
                    .into_iter()
                    .map(|i| {
                        Customer{
                            c_id: i, // c_id
                            c_d_id,
                            c_w_id,
                            c_first: (input.base_size + i).to_string(), // first name
                            c_middle: c_middle.clone(),
                            c_last: c_last.clone(),
                            c_street_1: None,
                            c_street_2: None,
                            c_city: None,
                            c_state: None,
                            c_zip: None,
                            c_phone: None,
                            c_since: None,
                            c_credit: None,
                            c_credit_lim: None,
                            c_discount: None,
                            c_balance: None,
                        }
                    })
                    .collect();

                let update_vals: Vec<Customer>
                = (0..input.updates_size)
                    .into_iter()
                    .map(|i| {
                        Customer{
                            c_id: i, // c_id
                            c_d_id,
                            c_w_id,
                            c_first: (input.base_size + i).to_string(), // first name
                            c_middle: c_middle.clone(),
                            c_last: c_last.clone(),
                            c_street_1: None,
                            c_street_2: None,
                            c_city: None,
                            c_state: None,
                            c_zip: None,
                            c_phone: None,
                            c_since: None,
                            c_credit: None,
                            c_credit_lim: None,
                            c_discount: None,
                            c_balance: None,
                        }
                    })
                    .collect();

                let txn_id = 1;
                let transaction_paramters = (
                    Some(txn_id),
                    Some(c_w_id),
                    Some(c_d_id),
                    Some(c_last),
                );

                return (base_vals, update_vals, transaction_paramters);
}

fn byname(c: &mut criterion::Criterion) {
    
    let c_last = "lastname".to_string();
    let c_d_id = 43;
    let c_w_id = 44;

    let base_size = 3000; // Number of customers per district
                          //let update_sizes = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    let update_sizes = vec![0, 1, 2, 4, 8, 16];
    let implementations = vec![
        ByNameImplementation::HydroFlowSortFold,
        ByNameImplementation::HydroFLowInlined,
    ];
    let inputs: Vec<_> = update_sizes
        .into_iter()
        .flat_map(|update_size| {
            implementations
                .iter()
                .map(move |&implementation| (update_size, implementation))
        })
        .map(|(updates_size, implementation)| ExperimentInput {
            base_size,
            updates_size,
            implementation,
        })
        .collect();

    let mut group = c.benchmark_group("Byname");

    for input in inputs {
        group.throughput(criterion::Throughput::Elements(input.updates_size as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{}", input.implementation), input),
            &input,
            |b, input| {
                let (base_vals, update_vals, _transaction_paramters) = generate_data(input);
                
                b.iter_batched(|| {
                    let combined_data = Vec::from_iter(base_vals.clone().into_iter().chain(update_vals.clone().into_iter()));

                    let (flow, flow_updates) = match input.implementation {
                        ByNameImplementation::HydroFlowSortFold => {
                            let flow = medianSortFold(base_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = medianSortFold(combined_data, c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                        ByNameImplementation::HydroFLowInlined => {
                            let flow = medianInlined(base_vals.clone(), c_last.clone(), c_d_id, c_w_id);
                            let flow_updates = medianInlined(combined_data, c_last.clone(), c_d_id, c_w_id);
                            (flow, flow_updates)
                        }
                    };

                    (flow, flow_updates)
                    
                }, |(mut flow, mut flow_updates)| {
                    flow.run_available();

                    if input.updates_size > 0 {
                        flow_updates.run_available();
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