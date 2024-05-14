use base::tpch::query_4::{LineItem, Order};
use chrono::NaiveDate;
use hydroflow_plus::*;
use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::HydroflowPlusMeta;
use stageleft::*;

pub fn query_4<'a, D: LocalDeploy<'a>>(
    flow: &FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>
) -> D::Process {
    let process = flow.process(process_spec);

    // 1. Scan orders
    let orders_filtered = flow.source_iter(&process, orders)
    //let orders_filtered = process.source_iter(q!(vec![Order{order_key: 0, order_date: NaiveDate::from_ymd_opt(1993, 7, 1).unwrap(), order_priority: "prior".to_string()}]))
    // 1.2 Filter orders on o_orderdate >= '1993-07-01' and < '1993-10-01'
        .filter(q!(|order: &Order| {
        order.order_date >= NaiveDate::from_ymd_opt(1993, 7, 1).unwrap()
            && order.order_date < NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()
    }));

    // 2. Scan from lineitem.
    //let line_items_filtered = process.source_iter(q!(vec![LineItem{order_key: 0, receiptdate: NaiveDate::from_ymd_opt(1993, 7, 1).unwrap(), commit_date: NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()}]))
    let line_items_filtered = flow.source_iter(&process, lineitem)
        // 2.2 Filter lineitem on l_commitdate < l_receiptdate
        .filter(q!(|line_item: &LineItem| line_item.commit_date < line_item.receiptdate));

    // 3. Join the two. o_orderkey = l_orderkey, payload: o_orderpriority
    // TODO: How to define the build side?
    // TODO: Multiset join in HF+?
    // XXX: Semijoin?
    // Build side: Orders?
    // Probe side: LineItem?
    let line_items_join = line_items_filtered.map(q!(|l|(l.order_key, None::<u8>)));
    let joined = orders_filtered.map(q!(|e|(e.order_key, e.order_priority))).join(line_items_join);
    // XXX: Join only outputs unique values? Where did we take these semantics?

    // 4. Aggregate.
    // Hash aggregation in DuckDB
    // 4.1 Group by: "o_orderpriority"
    // 4.2 Count
    // XXX: Why is it legal to return a value when it is not used afterwards?
    let agg = joined.map(q!(|x|(x.1.0, 1))).reduce_keyed(q!(|acc, x| *acc = *acc + x));

    // 5. Print: "o_orderpriority", "order_count"
    agg.for_each(q!(|x| println!("{:?}", x)));

    process
}

#[stageleft::entry]
pub fn query_4_runtime<'a>(
    flow: FlowBuilder<'a, SingleProcessGraph>,
    _cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    query_4(&flow, &(), lineitem, orders);
    flow.extract().optimize_default()
}

#[stageleft::runtime]
#[cfg(test)]
mod tests {
    use hydro_deploy::{Deployment, HydroflowCrate};
    use hydroflow_plus::futures::StreamExt;
    use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec};

    #[tokio::test]
    async fn test_query_4() {
        let mut deployment = Deployment::new();
        let localhost = deployment.Localhost();

        let flow = hydroflow_plus::FlowBuilder::new();
        let orders = stageleft::RuntimeData::new(&"FAKE");
        let lineitem = stageleft::RuntimeData::new(&"FAKE");
        let second_process = super::query_4(
            &flow,
            &DeployProcessSpec::new(|| {
                deployment.add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin("query_4")
                        .profile("dev"),
                )
            }),
            lineitem,
            orders
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
        /* assert_eq!(
            vec!["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        ); */
    }
}