use chrono::NaiveDate;
use hydroflow::hydroflow_syntax;

use base::tpch::query_1::{LineItem, LineItem2, LineItemAgg1, LineItemAgg2, query as query_base_original};

pub fn query(line_items: Vec<LineItem>) {

    let mut flow = hydroflow_syntax! {
        // 1. Scan from lineitem: "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"
        line_items = source_iter(line_items);

        // 2. Filter l_shipdate <= date '1998-12-01' - interval '90' day i.e., l_shipdate <= '1998-09-02'.
        // (2.1 Evaluate expression.)
        // 2.2. Filter on the expression. Need everything apart from l_shipdate.
        line_items_filtered = line_items -> filter(|x| x.l_shipdate <= NaiveDate::from_ymd_opt(1998, 9, 2).unwrap());

        // 3. Evaluate expressions for the aggregations.
        // XXX: Skipping projection of l_tax, could drop that column.
        line_items_proj = line_items_filtered -> map(|x| {
            // Project the fields.
            let x: LineItem2 = x.into();
            // l_extendedprice * (1 - l_discount) AS disc_price,
            let disc_price = x.l_extendedprice * (1.0 - x.l_discount);
            // l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge,
            let charge = disc_price * (1.0 + x.l_tax);
            return (x, disc_price, charge)
        });

        // 4. Group by l_returnflag, l_linestatus & compute aggregates.
        agg = line_items_proj -> map(|(x, disc_price, charge)| {
                // Group by l_returnflag, l_linestatus
                ((x.l_returnflag, x.l_linestatus), (x.l_quantity, x.l_extendedprice, disc_price, charge))
            })
            -> fold_keyed(Default::default, |acc: &mut LineItemAgg1, x| {
                // Hash aggregate without average
                let (l_quantity, l_extendedprice, disc_price, charge) = x;
                acc.sum_qty += l_quantity;
                acc.sum_base_price += l_extendedprice;
                acc.sum_disc_price += disc_price;
                acc.sum_charge += charge;
                acc.count_order += 1;
            })
            -> map(|(key, value)| {
                // Finalize aggregation with average
                let (l_returnflag, l_linestatus) = key;
                let value: LineItemAgg2 = value.into();
                ((l_returnflag, l_linestatus), value)
            });

        // 5. Sort by l_returnflag, l_linestatus.
        // XXX: InkFuse is skipping this step.
        ordered = agg -> sort_by_key(|x| &x.0);

        // Attach the sink for printing.
        ordered -> for_each(|((l_returnflag, l_linestatus), x): ((char, char), LineItemAgg2)| {
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
    };

    flow.run_available();
}

pub fn query_base(line_items: Vec<LineItem>) {

    let mut flow = hydroflow_syntax! {
        source_iter([line_items]) -> for_each(|line_items|{
            query_base_original(line_items);
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

        let line_items = LineItem::load(&conn, Some(1000));

        super::query(line_items);
    }
}