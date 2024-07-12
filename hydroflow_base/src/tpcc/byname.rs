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

impl PartialEq for Customer {
    fn eq(&self, other: &Self) -> bool {
        self.c_id == other.c_id
    }
}

impl PartialOrd for Customer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.c_first.cmp(&other.c_first))
    }
}

// TODO: Update reverse order generation for DBSP!
pub fn generate_customers(
    base_size: i32,
    update_size: i32,
    delete_size: i32,
) -> (
    Vec<Customer>,
    Vec<Customer>,
    Vec<Customer>,
    (Option<i32>, Option<i32>, Option<i32>, Option<String>),
) {
    /*
    c_id INT,
    c_d_id INT,
    c_w_id INT,
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
     */
    let c_d_id = 43;
    let c_w_id = 44;
    //let c_first = "first_name".to_string();
    let c_middle = "".to_string();
    let c_last = "lastname".to_string();

    let base_vals: Vec<Customer> = (0..base_size).rev()
        .into_iter()
        .map(|i| {
            Customer {
                c_id: i, // c_id
                c_d_id,
                c_w_id,
                c_first: (i).to_string(), // first name
                c_middle: c_middle.clone(),
                c_last: c_last.clone(),
                c_street_1: None,
                c_street_2: None,
                c_city: None,
                c_state: None,
                c_zip: None,
                c_phone: None,
                c_since: None,
                c_credit: None,
                c_credit_lim: None,
                c_discount: None,
                c_balance: None,
            }
        })
        .collect();

    let update_vals: Vec<Customer> = (0..update_size).rev()
        .into_iter()
        .map(|i| {
            Customer {
                c_id: (base_size + i), // c_id
                c_d_id,
                c_w_id,
                c_first: (base_size + i).to_string(), // first name
                c_middle: c_middle.clone(),
                c_last: c_last.clone(),
                c_street_1: None,
                c_street_2: None,
                c_city: None,
                c_state: None,
                c_zip: None,
                c_phone: None,
                c_since: None,
                c_credit: None,
                c_credit_lim: None,
                c_discount: None,
                c_balance: None,
            }
        })
        .collect();

    // Delete the median of the base data
    let median_index = base_size as usize / 2;
    let delete_vals = Vec::from_iter(base_vals[median_index..median_index + delete_size as usize].iter().cloned());

    let txn_id = 1;
    let transaction_paramters = (Some(txn_id), Some(c_w_id), Some(c_d_id), Some(c_last));

    return (base_vals, update_vals, delete_vals, transaction_paramters);
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
pub fn median_sort_fold(
    customer_data: Vec<Customer>,
    c_last: String,
    c_d_id: i32,
    c_w_id: i32,
) -> Hydroflow<'static> {
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

pub fn median_inlined(
    customer_data: Vec<Customer>,
    c_last: String,
    c_d_id: i32,
    c_w_id: i32,
) -> Hydroflow<'static> {
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

pub fn merge_sorted<T: std::cmp::PartialOrd + Clone>(a: Vec<T>, b: Vec<T>) -> Vec<T> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    let mut a_val = a.next();
    let mut b_val = b.next();
    while let (Some(a_val_inner), Some(b_val_inner)) = (a_val.clone(), b_val.clone()) {
        if a_val_inner < b_val_inner {
            result.push(a_val_inner);
            a_val = a.next();
        } else {
            result.push(b_val_inner);
            b_val = b.next();
        }
    }
    if let Some(a_val_inner) = a_val {
        result.push(a_val_inner);
        result.extend(a);
    }
    if let Some(b_val_inner) = b_val {
        result.push(b_val_inner);
        result.extend(b);
    }

    result
}

pub fn median_incremental_sort_inlined(
    customer_data: Vec<Customer>,
    customer_data_update: Vec<Customer>,
    _customer_data_deletion: Vec<Customer>,
    c_last: String,
    c_d_id: i32,
    c_w_id: i32,
) -> Hydroflow<'static> {

    let flow = hydroflow_syntax! {

        // Scan customer table
        customer = union() -> map(|x: Customer| (x.c_id + x.c_d_id + x.c_w_id, x));
        // Base data
        source_iter(customer_data) -> customer;
        // Update data
        source_iter(customer_data_update) -> defer_tick() -> customer;

        //customer_deletion = source_iter(customer_data_deletion) -> map(|x: Customer| x.c_id + x.c_d_id + x.c_w_id) -> defer_tick();

        // Filter customer
        customer_filtered = customer
            -> filter(|(_key, x): &(i32, Customer)| x.c_last == c_last && x.c_d_id == c_d_id && x.c_w_id == c_w_id);

        // Aggregate the new customers and sort them
        customer_agg = customer_filtered
        // Fold
        -> fold(|| Vec::<_>::new(), |acc: &mut Vec<_>, x: (_, Customer)| {
            acc.push(x);
            }) -> map(|mut x| {
                x.sort_by(|a, b| a.1.c_first.cmp(&b.1.c_first));
                x
            })
        // Filter empty vectors
        -> filter(|x: &Vec<(_, Customer)>| x.len() > 0);

        // Remove deletions from the previous aggregation
        /* customer_agg_prev -> flatten()
            -> [pos]customer_agg_prev_w_deletions;
        customer_deletion
            -> [neg]customer_agg_prev_w_deletions;

        customer_agg_prev_w_deletions = anti_join_multiset()
            -> fold(|| Vec::<(_, Customer)>::new(), |acc: &mut Vec<_>, x: (_, Customer)| {
                acc.push(x);
            }); */

        customer_agg_prev_w_deletions = customer_agg_prev;
        
        // Merge the previous aggregation with the new one
        customer_agg_prev_w_deletions -> /* inspect(|x| println!("Prev len: {}", x.len())) -> */ [0]customer_agg_merged;
        customer_agg -> /* inspect(|x| println!("Input len: {}", x.len())) -> */ [1]customer_agg_merged;
        customer_agg_merged = zip() -> map(|(prev, update)| merge_sorted(prev, update));
        // -> inspect(|x| println!("Merged len: {}", x.len()))
        /* -> inspect(|x| {
            for (_, c) in x {
                println!("Merged firstname: {}", c.c_first);
            }
        }); */

        // Select the median and pass on the aggregation
        customer_demux = customer_agg_merged -> demux(|x: Vec<(_, Customer)>, var_args!(agg, median)|
            match x {
                _ => {
                    let med = x[x.len() / 2].clone();
                    median.give(med);
                    agg.give(x);
                }
            }
        );

        // Keep the previous version of the aggregation to merge in the next tick
        customer_agg_prev = union();
        // Initialize with an empty vector
        source_iter(vec![Vec::<(_, Customer)>::new()]) -> customer_agg_prev;
        customer_demux[agg] -> defer_tick() -> customer_agg_prev;

        customer_demux[median]
            //-> inspect(|x| println!("{} Median: {:?}", context.current_tick(), x))
            -> for_each(|x: (_, Customer)| {
            black_box(x);
        });

    };

    flow
}
pub fn median_incremental_sort_inlined_with_deletions(
    customer_data: Vec<Customer>,
    customer_data_update: Vec<Customer>,
    customer_data_deletion: Vec<Customer>,
    c_last: String,
    c_d_id: i32,
    c_w_id: i32,
) -> Hydroflow<'static> {

    let flow = hydroflow_syntax! {

        // Scan customer table
        customer = union() -> map(|x: Customer| (x.c_id + x.c_d_id + x.c_w_id, x));
        // Base data
        source_iter(customer_data) -> customer;
        // Update data
        source_iter(customer_data_update) -> defer_tick() -> customer;

        customer_deletion = source_iter(customer_data_deletion) -> map(|x: Customer| x.c_id + x.c_d_id + x.c_w_id) -> defer_tick();

        // Filter customer
        customer_filtered = customer
            -> filter(|(_key, x): &(i32, Customer)| x.c_last == c_last && x.c_d_id == c_d_id && x.c_w_id == c_w_id);

        // Aggregate the new customers and sort them
        customer_agg = customer_filtered
        // Fold
        -> fold(|| Vec::<_>::new(), |acc: &mut Vec<_>, x: (_, Customer)| {
            acc.push(x);
            }) -> map(|mut x| {
                x.sort_by(|a, b| a.1.c_first.cmp(&b.1.c_first));
                x
            })
        // Filter empty vectors
        -> filter(|x: &Vec<(_, Customer)>| x.len() > 0);

        // Remove deletions from the previous aggregation
        customer_agg_prev -> flatten()
            -> [pos]customer_agg_prev_w_deletions;
        customer_deletion
            -> [neg]customer_agg_prev_w_deletions;

        customer_agg_prev_w_deletions = anti_join_multiset()
            -> fold(|| Vec::<(_, Customer)>::new(), |acc: &mut Vec<_>, x: (_, Customer)| {
                acc.push(x);
            });
        
        // Merge the previous aggregation with the new one
        customer_agg_prev_w_deletions -> /* inspect(|x| println!("Prev len: {}", x.len())) -> */ [0]customer_agg_merged;
        customer_agg -> /* inspect(|x| println!("Input len: {}", x.len())) -> */ [1]customer_agg_merged;
        customer_agg_merged = zip() -> map(|(prev, update)| merge_sorted(prev, update));
        // -> inspect(|x| println!("Merged len: {}", x.len()))
        /* -> inspect(|x| {
            for (_, c) in x {
                println!("Merged firstname: {}", c.c_first);
            }
        }); */

        // Select the median and pass on the aggregation
        customer_demux = customer_agg_merged -> demux(|x: Vec<(_, Customer)>, var_args!(agg, median)|
            match x {
                _ => {
                    let med = x[x.len() / 2].clone();
                    median.give(med);
                    agg.give(x);
                }
            }
        );

        // Keep the previous version of the aggregation to merge in the next tick
        customer_agg_prev = union();
        // Initialize with an empty vector
        source_iter(vec![Vec::<(_, Customer)>::new()]) -> customer_agg_prev;
        customer_demux[agg] -> defer_tick() -> customer_agg_prev;

        customer_demux[median]
            //-> inspect(|x| println!("{} Median: {:?}", context.current_tick(), x))
            -> for_each(|x: (_, Customer)| {
            black_box(x);
        });

    };

    flow
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_median_incremental_sort_inlined() {
        let (base_vals, update_vals, delete_vals, transaction_paramters) =
            super::generate_customers(10, 3, 2);

        let mut flow = super::median_incremental_sort_inlined(
            base_vals, update_vals, delete_vals,
            transaction_paramters.3.clone().unwrap(),
            transaction_paramters.2.unwrap(),
            transaction_paramters.1.unwrap(),
        );

        flow.run_available();
    }

    #[test]
    fn test_median_incremental_sort_inlined_with_deletions() {
        let (base_vals, update_vals, delete_vals, transaction_paramters) =
            super::generate_customers(10, 3, 2);

        let mut flow = super::median_incremental_sort_inlined_with_deletions(
            base_vals, update_vals, delete_vals,
            transaction_paramters.3.clone().unwrap(),
            transaction_paramters.2.unwrap(),
            transaction_paramters.1.unwrap(),
        );

        flow.run_available();
    }

    #[test]
    fn test_merge_sorted() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 4, 6, 8, 10];
        let result = super::merge_sorted(a, b);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
}