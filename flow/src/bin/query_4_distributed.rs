use base::tpch::initialize::initialize_database;

#[tokio::main]
async fn main() {

    hydroflow_plus::util::cli::launch!(|ports| {
        
        // Load query data
        let scale_factor = 1;
        let conn = initialize_database(scale_factor);
        let (lineitem, orders) = base::tpch::query_4::load(&conn);

        flow::tpch::query_4_distributed::query_4_distributed_runtime!(ports, lineitem, orders, false)
    })
    .await;
}
