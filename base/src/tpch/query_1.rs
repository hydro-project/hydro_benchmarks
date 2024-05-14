use std::collections::HashMap;

use chrono::NaiveDate;
use duckdb::Connection;
use itertools::Itertools;

use super::util::to_date;

pub struct LineItem {
    pub l_returnflag: char,
    pub l_linestatus: char,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_shipdate: NaiveDate,
}

impl LineItem {
    pub fn load(conn: &Connection, limit: Option<u32>) -> Vec<Self> {
        // Create iterator for LineItem
        let query = match limit {
            Some(limit) => format!("SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate FROM lineitem LIMIT {};", limit),
            None => "SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate FROM lineitem;".to_string(),
        };
        let mut stmt = conn
            .prepare(&query)
            .expect("Error preparing query for LineItem");
        let line_items = stmt
            .query_map([], |row| {
                Ok(LineItem {
                    l_returnflag: row.get::<_, String>(0)?.chars().next().unwrap(),
                    l_linestatus: row.get::<_, String>(1)?.chars().next().unwrap(),
                    l_quantity: row.get(2)?,
                    l_extendedprice: row.get(3)?,
                    l_discount: row.get(4)?,
                    l_tax: row.get(5)?,
                    l_shipdate: to_date(row.get(6)?),
                })
            })
            .expect("Error querying LineItem");

        let line_items = line_items.filter(|x| x.is_ok()).map(|x| x.unwrap());

        line_items.collect()
    }
}

pub struct LineItem2 {
    pub l_returnflag: char,
    pub l_linestatus: char,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
}

impl Into<LineItem2> for LineItem {
    fn into(self) -> LineItem2 {
        LineItem2 {
            l_returnflag: self.l_returnflag,
            l_linestatus: self.l_linestatus,
            l_quantity: self.l_quantity,
            l_extendedprice: self.l_extendedprice,
            l_discount: self.l_discount,
            l_tax: self.l_tax,
        }
    }
}

pub struct LineItemAgg1 {
    pub sum_qty: f64,
    pub sum_base_price: f64,
    pub sum_disc_price: f64,
    pub sum_charge: f64,
    pub count_order: u64,
}

impl Default for LineItemAgg1 {
    fn default() -> Self {
        LineItemAgg1 {
            sum_qty: 0.0,
            sum_base_price: 0.0,
            sum_disc_price: 0.0,
            sum_charge: 0.0,
            count_order: 0,
        }
    }
}

#[derive(Debug)]
pub struct LineItemAgg2 {
    pub sum_qty: f64,
    pub sum_base_price: f64,
    pub sum_disc_price: f64,
    pub sum_charge: f64,
    pub avg_qty: f64,
    pub avg_price: f64,
    pub avg_disc: f64,
    pub count_order: u64,
}

impl From<LineItemAgg1> for LineItemAgg2 {
    fn from(x: LineItemAgg1) -> LineItemAgg2 {
        LineItemAgg2 {
            sum_qty: x.sum_qty,
            sum_base_price: x.sum_base_price,
            sum_disc_price: x.sum_disc_price,
            sum_charge: x.sum_charge,
            avg_qty: x.sum_qty / x.count_order as f64,
            avg_price: x.sum_base_price / x.count_order as f64,
            avg_disc: x.sum_disc_price / x.count_order as f64,
            count_order: x.count_order,
        }
    }
}

pub fn load(conn: &Connection) -> Vec<LineItem> {
    LineItem::load(conn, None)
}

pub fn query(line_items: Vec<LineItem>) {

    // 1. Scan from lineitem: "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"
    let line_items = line_items.into_iter();

    // 2. Filter l_shipdate <= date '1998-12-01' - interval '90' day i.e., l_shipdate <= '1998-09-02'.
    // (2.1 Evaluate expression.)
    // 2.2. Filter on the expression. Need everything apart from l_shipdate.
    let line_items_filtered = line_items.filter(|x| x.l_shipdate <= NaiveDate::from_ymd_opt(1998, 9, 2).unwrap());

    // 3. Evaluate expressions for the aggregations.
    // XXX: Skipping projection of l_tax, cloud drop that column.
    let line_items_proj = line_items_filtered.map(|x| {
        // Project the fields.
        let x: LineItem2 = x.into();
        // l_extendedprice * (1 - l_discount) AS disc_price,
        let disc_price = x.l_extendedprice * (1.0 - x.l_discount);
        // l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge,
        let charge = disc_price * (1.0 + x.l_tax);
        return (x, disc_price, charge)
    });

    // 4. Group by l_returnflag, l_linestatus & compute aggregates.
    let agg = line_items_proj.map(|(x, disc_price, charge)| {
        // Group by l_returnflag, l_linestatus
        ((x.l_returnflag, x.l_linestatus), x.l_quantity, x.l_extendedprice, disc_price, charge)
    }).fold(HashMap::new(), |mut acc, x| {
        // Hash aggregate without average
        let (key, l_quantity, l_extendedprice, disc_price, charge) = x;
        let entry = acc.entry(key).or_insert(LineItemAgg1 {
            sum_qty: 0.0,
            sum_base_price: 0.0,
            sum_disc_price: 0.0,
            sum_charge: 0.0,
            count_order: 0,
        });
        entry.sum_qty += l_quantity;
        entry.sum_base_price += l_extendedprice;
        entry.sum_disc_price += disc_price;
        entry.sum_charge += charge;
        entry.count_order += 1;
        acc
    }).into_iter().map(|(key, value)| {
        // Finalize aggregation with average
        let (l_returnflag, l_linestatus) = key;
        let value: LineItemAgg2 = value.into();
        ((l_returnflag, l_linestatus), value)
    });

    // 5. Sort by l_returnflag, l_linestatus.
    // XXX: InkFuse is skipping this step.
    let ordered = agg.sorted_by_key(|x| x.0);

    // Attach the sink for printing.
    ordered.for_each(|x| {
        let ((l_returnflag, l_linestatus), x) = x;
        println!("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}",
                 l_returnflag,
                 l_linestatus,
                 x.sum_qty,
                 x.sum_base_price,
                 x.sum_disc_price,
                 x.sum_charge,
                 x.avg_qty,
                 x.avg_price,
                 x.avg_disc,
                 x.count_order,
        );
    });
}

pub fn query_duckdb(conn: &Connection, limit: Option<u32>) {
    let table = match limit {
        Some(limit) => format!("(SELECT * FROM lineitem LIMIT {})", limit),
        None => "lineitem".to_string(),
    };
    let mut stmt = conn.prepare(&format!(r#"
        SELECT
            l_returnflag,
            l_linestatus,
            sum(l_quantity) AS sum_qty,
            sum(l_extendedprice) AS sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            avg(l_quantity) AS avg_qty,
            avg(l_extendedprice) AS avg_price,
            avg(l_discount) AS avg_disc,
            count(*) AS count_order
        FROM
            {}
        WHERE
            l_shipdate <= CAST('1998-09-02' AS date)
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag,
            l_linestatus;
    "#, table)).expect("Error preparing query for LineItem");
    let mut rows = stmt.query([]).expect("Error executing Query 1");
    while let Some(row) = rows.next().unwrap() {
        let l_returnflag: char = row.get::<_, String>(0).unwrap().chars().next().unwrap();
        let l_linestatus: char = row.get::<_, String>(1).unwrap().chars().next().unwrap();
        let sum_qty: f64 = row.get(2).unwrap();
        let sum_base_price: f64 = row.get(3).unwrap();
        let sum_disc_price: f64 = row.get(4).unwrap();
        let sum_charge: f64 = row.get(5).unwrap();
        let avg_qty: f64 = row.get(6).unwrap();
        let avg_price: f64 = row.get(7).unwrap();
        let avg_disc: f64 = row.get(8).unwrap();
        let count_order: u64 = row.get(9).unwrap();
        println!("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}",
                 l_returnflag,
                 l_linestatus,
                 sum_qty,
                 sum_base_price,
                 sum_disc_price,
                 sum_charge,
                 avg_qty,
                 avg_price,
                 avg_disc,
                 count_order,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tpch::initialize::initialize_database;

    #[test]
    fn test_query_1() {
        let conn = initialize_database(1);
        let line_items = LineItem::load(&conn, Some(1000));
        query(line_items);
    }

    #[test]
    fn test_query_1_duckdb() {
        let conn = initialize_database(1);
        query_duckdb(&conn, Some(1000));
    }
}