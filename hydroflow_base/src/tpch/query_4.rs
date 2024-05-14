use chrono::NaiveDate;
use hydroflow::hydroflow_syntax;

use base::tpch::query_4::{LineItem, Order, query as query_base_original};

pub fn query(line_items: Vec<LineItem>, orders: Vec<Order>) {

    let mut flow = hydroflow_syntax! {
        // 1. Scan orders
        orders_filtered = source_iter(orders)
        // 1.2 Filter orders on o_orderdate >= '1993-07-01' and < '1993-10-01'
        -> filter(|order| {
            order.order_date >= NaiveDate::from_ymd_opt(1993, 7, 1).unwrap()
                && order.order_date < NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()
        });

        // 2. Scan from lineitem.
        line_items_filtered = source_iter(line_items)
            // 2.2 Filter lineitem on l_commitdate < l_receiptdate
            -> filter(|line_item| line_item.commit_date < line_item.receiptdate);

        // 3. Join the two. o_orderkey = l_orderkey, payload: o_orderpriority
        // XXX: Semijoin?
        // Build side: Orders?
        // Probe side: LineItem?
        orders_filtered -> map(|e|(e.order_key, e.order_priority)) -> [0]joined;
        line_items_filtered -> map(|l|(l.order_key, None::<u8>)) -> [1]joined;
        // Note: Implementing a semijoin using a hash join with unique output.
        joined = join() -> map(|x| x.1.0);

        // 4. Aggregate.
        // Hash aggregation in DuckDB
        // 4.1 Group by: "o_orderpriority"
        // 4.2 Count
        // XXX: Why is it legal to return a value when it is not used afterwards?
        agg = joined -> map(|x: String| (x, 1)) -> reduce_keyed(|acc, x| *acc = *acc + x);

        // 5. Print: "o_orderpriority", "order_count"
        agg -> for_each(|x| println!("{:?}", x));
    };

    flow.run_available();
}

pub fn query_base(line_items: Vec<LineItem>, orders: Vec<Order>) {

    let mut flow = hydroflow_syntax! {
        source_iter([(line_items, orders)]) -> for_each(|(line_items, orders)|{
            query_base_original(line_items, orders);
        });
    };

    flow.run_available();
}

#[cfg(test)]
mod tests {
    use super::*;
    use base::tpch::initialize::initialize_database;

    #[test]
    fn test_query() {
        let conn = initialize_database(1);

        let limit = Some(1000);

        let line_items = LineItem::load(&conn, limit);
        let orders = Order::load(&conn, limit);

        super::query(line_items, orders);
    }

    #[test]
    fn test_query_base() {
        let conn = initialize_database(1);

        let limit = Some(1000);

        let line_items = LineItem::load(&conn, limit);
        let orders = Order::load(&conn, limit);

        super::query(line_items, orders);
    }
}