use flow::tpch::query_4::query_4;
use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow::futures::StreamExt;
use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec};

#[tokio::main]
async fn main() {
    let profile = "dev";
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydroflow_plus::FlowBuilder::new();
    let orders = stageleft::RuntimeData::new(&"FAKE");
    let lineitem = stageleft::RuntimeData::new(&"FAKE");
    let second_process = query_4(
        &flow,
        &DeployProcessSpec::new(|| {
            deployment.add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("query_4")
                    .profile(profile),
            )
        }),
        lineitem,
        orders,
    );

    println!("Deploying");
    deployment.deploy().await.unwrap();

    println!("Getting stdout");
    let second_process_stdout = second_process.stdout().await;

    println!("Starting");
    deployment.start().await.unwrap();

    println!("Collecting");
    let res = second_process_stdout.take(5).collect::<Vec<_>>().await;
    println!("{:?}", res);
}
