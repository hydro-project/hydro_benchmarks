use std::hash::{Hash, Hasher};

use __staged::stream::Windowed;
use serde::{de::DeserializeOwned, Serialize};
use base::tpch::query_4::{LineItem, Order};
use chrono::NaiveDate;
use hydroflow_plus::*;
use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};
use stageleft::*;

/* Inputs on single machine 
 */
fn distributed_join<'a, Key: Hash + Eq + Serialize + DeserializeOwned, T: Serialize + DeserializeOwned, U: Serialize + DeserializeOwned, D: Deploy<'a, ClusterId = u32>>(build: Stream<'a, (Key, T), Windowed, D::Process>, probe: Stream<'a, (Key, U), Windowed, D::Process>, cluster: &D::Cluster) -> Stream<'a, (Key, (T, U)), Windowed, D::Cluster> {

    // Note: Let compiler move the persist after the broadcast when user puts it before the broadcast
    let build = build.broadcast_bincode(cluster).all_ticks();

    let all_ids_vec = cluster.ids();

    let probe = probe.map(q!(|(key, value)| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let id = (hasher.finish() % all_ids_vec.len() as u64) as u32;

        (id, (key, value))

    })).send_bincode(cluster);

    let joined = build.join(probe.tick_batch());

    joined
}

pub fn query_4_distributed<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>,
    intermediate_aggregation: bool,
) -> D::Process {
    let process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    println!("Query 4 distributed with intermediate aggregation: {}", intermediate_aggregation);

    // 1. Scan orders
    let orders_filtered = flow.source_iter(&process, orders)
    // 1.2 Filter orders on o_orderdate >= '1993-07-01' and < '1993-10-01'
        .filter(q!(|order: &Order| {
        order.order_date >= NaiveDate::from_ymd_opt(1993, 7, 1).unwrap()
            && order.order_date < NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()
    }));

    // 2. Scan from lineitem.
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
    let orders_join = orders_filtered.map(q!(|e|(e.order_key, e.order_priority)));

    // Distributed join
    //let joined = orders_join.join(line_items_join);
    let joined = distributed_join::<_, _, _, D>(orders_join, line_items_join, &cluster);

    // 4. Aggregate.
    // Hash aggregation in DuckDB
    // 4.1 Group by: "o_orderpriority"
    // 4.2 Count
    let agg = joined.map(q!(|x|(x.1.0, 1)));

    // Example of alternate code generation
    // Note: Let compiler replicate reduction before the send when commutative
    let agg = if intermediate_aggregation {
        // Intermediate aggregation
        agg.reduce_keyed(q!(|acc, x| *acc = *acc + x))
    } else {
        agg
    };

    // Collect the results and reduce again over all ticks
    let agg = agg.send_bincode_interleaved(&process).all_ticks().reduce_keyed(q!(|acc, x| *acc = *acc + x));

    // 5. Print: "o_orderpriority", "order_count"
    // Reduce to single output and then print all at once
    agg.fold(q!(|| vec![]), q!(|acc, x| {acc.push(x);}))
        .for_each(q!(|x: Vec<(String, i32)>| {
            let mut x = x;
            x.sort_by(|a, b| a.0.cmp(&b.0));
            println!("{:?}", x);
        }));

    process
}

pub fn query_4_distributed_partitioned<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>,
    intermediate_aggregation: bool,
) -> D::Process {
    let process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    println!("Query 4 distributed with intermediate aggregation: {}", intermediate_aggregation);

    let all_ids_vec = cluster.ids();

    // 1. Scan orders
    let orders_filtered = flow.source_iter(&cluster, orders)
    // 1.2 Filter orders on o_orderdate >= '1993-07-01' and < '1993-10-01'
        .filter(q!(|order: &Order| {
        order.order_date >= NaiveDate::from_ymd_opt(1993, 7, 1).unwrap()
            && order.order_date < NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()
    }));

    // 2. Scan from lineitem.
    let line_items_filtered = flow.source_iter(&cluster, lineitem)
        // Filter out hash partition of this cluster node
        .filter(q!(|lineitem: &LineItem| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            lineitem.order_key.hash(&mut hasher);
            let id = (hasher.finish() % all_ids_vec.len() as u64) as u32;
            // TODO: Get ID of this cluster node
            let my_id = 32;
            //let my_id = cluster.id() as u32;

            id == my_id
        }))
        // 2.2 Filter lineitem on l_commitdate < l_receiptdate
        .filter(q!(|line_item: &LineItem| line_item.commit_date < line_item.receiptdate));

    // 3. Join the two. o_orderkey = l_orderkey, payload: o_orderpriority
    // XXX: Semijoin?
    // Build side: Orders?
    // Probe side: LineItem?
    let line_items_join = line_items_filtered.map(q!(|l|(l.order_key, None::<u8>)));
    let orders_join = orders_filtered.map(q!(|e|(e.order_key, e.order_priority)));

    // Local join
    let joined = orders_join.all_ticks().join(line_items_join);
    //let joined = distributed_join::<_, _, _, D>(orders_join, line_items_join, &cluster);

    // 4. Aggregate.
    // Hash aggregation in DuckDB
    // 4.1 Group by: "o_orderpriority"
    // 4.2 Count
    let agg = joined.map(q!(|x|(x.1.0, 1)));

    // Example of alternate code generation
    // Note: Let compiler replicate reduction before the send when commutative
    let agg = if intermediate_aggregation {
        // Intermediate aggregation
        agg.reduce_keyed(q!(|acc, x| *acc = *acc + x))
    } else {
        agg
    };

    // Collect the results and reduce again over all ticks
    let agg = agg.send_bincode_interleaved(&process).all_ticks().reduce_keyed(q!(|acc, x| *acc = *acc + x));

    // 5. Print: "o_orderpriority", "order_count"
    // Reduce to single output and then print all at once
    agg.fold(q!(|| vec![]), q!(|acc, x| {acc.push(x);}))
        .for_each(q!(|x: Vec<(String, i32)>| {
            let mut x = x;
            x.sort_by(|a, b| a.0.cmp(&b.0));
            println!("{:?}", x);
        }));

    process
}

#[stageleft::entry]
pub fn query_4_distributed_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>,
    intermediate_aggregation: bool,
) -> impl Quoted<'a, Hydroflow<'a>> {
    query_4_distributed(&flow, &cli, &cli, lineitem, orders, intermediate_aggregation);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}

#[stageleft::entry]
pub fn query_4_distributed_partitioned_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
    lineitem: RuntimeData<Vec<LineItem>>,
    orders: RuntimeData<Vec<Order>>,
    intermediate_aggregation: bool,
) -> impl Quoted<'a, Hydroflow<'a>> {
    query_4_distributed_partitioned(&flow, &cli, &cli, lineitem, orders, intermediate_aggregation);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}