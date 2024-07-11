use chrono::NaiveDate;
use criterion::black_box;
use hydroflow::{hydroflow_syntax, scheduled::graph::Hydroflow};

#[derive(Debug, Clone)]
pub struct Customer {
    pub c_id: i32,
    pub c_d_id: i32,
    pub c_w_id: i32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: Option<String>,
    pub c_street_2: Option<String>,
    pub c_city: Option<String>,
    pub c_state: Option<String>,
    pub c_zip: Option<String>,
    pub c_phone: Option<String>,
    pub c_credit: Option<String>,
    pub c_credit_lim: Option<f64>,
    pub c_discount: Option<f64>,
    pub c_balance: Option<f64>,
    pub c_since: Option<NaiveDate>,
}

/* 
    CREATE VIEW cust_agg AS
    SELECT ARRAY_AGG((c_id + c_w_id + c_d_id) ORDER BY c_first) AS cust_array
    FROM (SELECT c.c_id, c.c_w_id, c.c_d_id, c.c_first
        FROM customer AS c,
            transaction_parameters AS t
        WHERE c.c_last = t.c_last
            AND c.c_d_id = t.c_d_id
            AND c.c_w_id = t.c_w_id
        ORDER BY c_first);

    CREATE VIEW cust_byname AS
    SELECT c.c_first, c.c_middle, c.c_id,
        c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
        c.c_phone, c.c_credit, c.c_credit_lim,
        c.c_discount, c.c_balance, c.c_since
    FROM customer as c,
        cust_agg as a,
        transaction_parameters as t
    WHERE (c.c_id + c.c_w_id + c.c_d_id) = a.cust_array[(ARRAY_LENGTH(a.cust_array) / 2) + 1];

    */
pub fn medianSortFold(customer_data: Vec<Customer>, c_last: String, c_d_id: i32, c_w_id: i32) -> Hydroflow<'static> {

   let flow = hydroflow_syntax! {

        // Scan customer table
        customer = source_iter(customer_data);

        // Filter customer
        customer_filtered = customer
            -> filter(|x: &Customer| x.c_last == c_last && x.c_d_id == c_d_id && x.c_w_id == c_w_id);

        // Compute the median first name:
        customer_agg = customer_filtered
        // Sort
        -> sort_by_key(|x: &Customer| &x.c_first) // -> defer_tick()
        // Fold
        -> fold(|| Vec::<_>::new(), |acc: &mut Vec<_>, x: Customer| {
            acc.push(x);
            })
        -> map(|x| x[(x.len() / 2) + 1].clone());

        customer_agg -> for_each(|x: Customer| {
            black_box(x);
        });

    };

    flow
}

pub fn medianInlined(customer_data: Vec<Customer>, c_last: String, c_d_id: i32, c_w_id: i32) -> Hydroflow<'static> {

   let flow = hydroflow_syntax! {

        // Scan customer table
        customer = source_iter(customer_data);

        // Filter customer
        customer_filtered = customer
            -> filter(|x: &Customer| x.c_last == c_last && x.c_d_id == c_d_id && x.c_w_id == c_w_id);

        // Compute the median first name:
        customer_agg = customer_filtered
        // Fold
        -> fold(|| Vec::<_>::new(), |acc: &mut Vec<_>, x: Customer| {
            acc.push(x);
            })
        -> map(|mut x| {
            x.sort_by(|a, b| a.c_first.cmp(&b.c_first));
            x
        })
        -> map(|x| x[(x.len() / 2) + 1].clone());

        customer_agg -> for_each(|x: Customer| {
            black_box(x);
        });

    };

    flow
}

/* pub fn query_base(line_items: Vec<LineItem>) {

    let mut flow = hydroflow_syntax! {
        source_iter([line_items]) -> for_each(|line_items|{
            query_base_original(line_items);
        });
    };

    flow.run_available();
} */

/* #[cfg(test)]
mod tests {
    use super::*;
    use base::tpch::initialize::initialize_database;

    #[test]
    fn test_query() {
        let conn = initialize_database(1);

        let line_items = LineItem::load(&conn, Some(1000));

        super::query(line_items);
    }
} */