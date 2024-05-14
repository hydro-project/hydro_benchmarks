use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use duckdb::Connection;

use super::util::to_date;

#[derive(Debug, Clone)]
pub struct LineItem {
    pub order_key: i32, // BIGINT
    pub receiptdate: NaiveDate, // DATE
    pub commit_date: NaiveDate, // DATE
}

impl LineItem {
    pub fn load(conn: &Connection, limit: Option<u32>) -> Vec<Self> {
        // Create iterator for LineItem
        let query = match limit {
            Some(limit) => format!(
                "SELECT l_orderkey, l_receiptdate, l_commitdate FROM lineitem LIMIT {};",
                limit
            ),
            None => "SELECT l_orderkey, l_receiptdate, l_commitdate FROM lineitem;".to_string(),
        };
        let mut stmt = conn
            .prepare(&query)
            .expect("Error preparing query for LineItem");
        let line_items = stmt
            .query_map([], |row| {
                Ok(LineItem {
                    order_key: row.get(0)?,
                    receiptdate: to_date(row.get(1)?),
                    commit_date: to_date(row.get(2)?),
                })
            })
            .expect("Error querying LineItem");

        let line_items = line_items.filter(|x| x.is_ok()).map(|x| x.unwrap());

        line_items.collect()
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_key: i32,
    pub order_date: NaiveDate,
    pub order_priority: String,
}

impl Order {
    pub fn load(conn: &Connection, limit: Option<u32>) -> Vec<Self> {
        // Create iterator for Orders
        let query = match limit {
            Some(limit) => format!(
                "SELECT o_orderkey, o_orderdate, o_orderpriority FROM orders LIMIT {};",
                limit
            ),
            None => "SELECT o_orderkey, o_orderdate, o_orderpriority FROM orders;".to_string(),
        };
        let mut stmt = conn
            .prepare(&query)
            .expect("Error preparing query for LineItem");
        let orders = stmt
            .query_map([], |row| {
                Ok(Self {
                    order_key: row.get(0)?,
                    order_date: to_date(row.get(1)?),
                    order_priority: row.get(2)?,
                })
            })
            .expect("Error querying LineItem");

        let orders = orders.filter_map(|x| x.ok());

        orders.collect()
    }
}

pub fn load(conn: &Connection) -> (Vec<LineItem>, Vec<Order>) {
    let line_items = LineItem::load(&conn, None);
    let orders = Order::load(&conn, None);

    (line_items, orders)
}

pub fn query(line_items: Vec<LineItem>, orders: Vec<Order>) {

    // 1. Scan orders
    let orders_filtered = orders
        .into_iter()
        // 1.2 Filter orders on o_orderdate >= '1993-07-01' and < '1993-10-01'
        .filter(|order| {
            order.order_date >= NaiveDate::from_ymd_opt(1993, 7, 1).unwrap()
                && order.order_date < NaiveDate::from_ymd_opt(1993, 10, 1).unwrap()
        });

    // 2. Scan from lineitem.
    let line_items_filtered = line_items
        .into_iter()
        // 2.2 Filter lineitem on l_commitdate < l_receiptdate
        .filter(|line_item| line_item.commit_date < line_item.receiptdate);

    // 3. Join the two. o_orderkey = l_orderkey, payload: o_orderpriority
    // Build side: Orders?
    // Probe side: LineItem?
    // HashJoin?
    let join_build = line_items_filtered.fold(HashSet::new(), |mut acc, e| {
        acc.insert(e.order_key);
        acc
    });
    let joined =
        orders_filtered.filter_map(|o| join_build.get(&o.order_key).map(|_| o.order_priority));

    // 4. Aggregate.
    // Hash aggregation in DuckDB
    // 4.1 Group by: "o_orderpriority"
    // 4.2 Count
    let agg = joined.fold(HashMap::new(), |mut acc, e| {
        let count = acc.entry(e).or_insert(0);
        *count += 1;
        acc
    });

    // 5. Print: "o_orderpriority", "order_count"
    agg.into_iter().for_each(|x| println!("{:?}", x));
}

pub fn query_duckdb(conn: &Connection, limit: Option<u32>) {
    let orders_table = match limit {
        Some(limit) => format!("(SELECT * FROM orders LIMIT {})", limit),
        None => "orders".to_string(),
    };
    let lineitem_table = match limit {
        Some(limit) => format!("(SELECT * FROM lineitem LIMIT {})", limit),
        None => "lineitem".to_string(),
    };

    let mut stmt = conn
        .prepare(&format!(
            r#"
    SELECT
    o_orderpriority,
    count(*) AS order_count
    FROM
    {}
    WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
    SELECT
    *
    FROM
    {}
    WHERE
    l_orderkey = o_orderkey
    AND l_commitdate < l_receiptdate)
    GROUP BY
    o_orderpriority
    ORDER BY
    o_orderpriority;
"#,
            orders_table, lineitem_table
        ))
        .expect("Error preparing query for DuckDB");

    let mut rows = stmt.query([]).expect("Error executing Query 4");
    while let Some(row) = rows.next().unwrap() {
        let order_priority: String = row.get(0).unwrap();
        let order_count: i64 = row.get(1).unwrap();
        println!("{}, {}", order_priority, order_count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tpch::initialize::initialize_database;

    #[test]
    fn test_query_4() {
        let limit = None;

        let conn = initialize_database(1);
        let line_items = LineItem::load(&conn, limit);
        let orders = Order::load(&conn, limit);

        // Call the query function
        query(line_items, orders);
    }

    #[test]
    fn test_query_4_duckdb() {
        let conn = initialize_database(1);

        let res = conn
            .query_row(
                r#"
    SELECT
    count(*) AS order_count
    FROM
    orders
    WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    "#,
                [],
                |row| {
                    let order_count: i64 = row.get(0)?;
                    Ok(order_count)
                },
            )
            .unwrap();
        println!("Orders filtered: {:?}", res);

        let res = conn
            .query_row(
                r#"
    SELECT
    count(*)
    FROM
    lineitem
    WHERE
    l_commitdate < l_receiptdate
    "#,
                [],
                |row| {
                    let order_count: i64 = row.get(0)?;
                    Ok(order_count)
                },
            )
            .unwrap();
        println!("LineItem filtered: {:?}", res);

        query_duckdb(&conn, None);
    }

    #[test]
    fn test_load() {
        let conn = initialize_database(1);
        let line_items = LineItem::load(&conn, Some(1));
        let orders = Order::load(&conn, Some(1));

        assert_eq!(line_items.len(), 1);
        assert_eq!(orders.len(), 1);
    }
}
