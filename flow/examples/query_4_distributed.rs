use std::cell::RefCell;

use flow::tpch::query_4_distributed::query_4_distributed;
use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow::futures::StreamExt;
use hydroflow_plus_cli_integration::{DeployClusterSpec, DeployCrateWrapper, DeployProcessSpec};

#[tokio::main]
async fn main() {
    let intermediate_aggregation = true;
    let cluster_size = 2;
    let profile = "dev";

    let deployment = RefCell::new(Deployment::new());
    let localhost = deployment.borrow_mut().Localhost();

    let flow = hydroflow_plus::FlowBuilder::new();
    let orders = stageleft::RuntimeData::new(&"FAKE");
    let lineitem = stageleft::RuntimeData::new(&"FAKE");
    let second_process = query_4_distributed(
        &flow,
        &DeployProcessSpec::new(|| {
            deployment.borrow_mut().add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("query_4_distributed")
                    //.perf("~/query_4_perf.dat".into()).profile("profile"),
                    .profile(profile),
            )
        }),
        &DeployClusterSpec::new(|| {
            (0..cluster_size)
                .map(|_| {
                    deployment.borrow_mut().add_service(
                        HydroflowCrate::new(".", localhost.clone())
                            .bin("query_4_distributed")
                            //.perf("~/query_4_perf.dat".into()).profile("profile"),
                            .profile(profile),
                    )
                })
                .collect()
        }),
        lineitem,
        orders,
        intermediate_aggregation,
    );

    let mut deployment = deployment.into_inner();

    println!("Deploying");
    deployment.deploy().await.unwrap();

    println!("Getting stdout");
    let mut second_process_stdout = second_process.stdout().await;

    println!("Starting");
    deployment.start().await.unwrap();

    println!("Collecting");
    
    let expected = vec![
        ("1-URGENT", 10594),
        ("2-HIGH", 10476),
        ("3-MEDIUM", 10410),
        ("4-NOT SPECIFIED", 10556),
        ("5-LOW", 10487),
    ];
    let expected = format!("{:?}", expected);
    println!("Expected: {:?}", expected);
    while let Some(res) = second_process_stdout.next().await {
        println!("{:?}", res);

        // Check if fixed point reached then break
        if res == expected {
            break;
        }
    }
}
