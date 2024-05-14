use base::tpch::initialize::initialize_database;

#[tokio::main]
async fn main() {
    //flow::tpch::query_4::query_4_runtime!().run_async().await;
    hydroflow_plus::util::cli::launch!(|ports| {
        //let orders: Vec<Order> = vec![];
        
        // Load query data
        let scale_factor = 1;
        let conn = initialize_database(scale_factor);
        let (lineitem, orders) = base::tpch::query_4::load(&conn);
        //let orders = Order::load(&conn, Some(1000));

        flow::tpch::query_4::query_4_runtime!(ports, lineitem, orders)
    })
    .await;
}
