use hydroflow::hydroflow_syntax;

use base::tpch::query_19::{Part, LineItem, query as query_base_original};

pub fn query(line_items: Vec<LineItem>, part: Vec<Part>) {

    let mut flow = hydroflow_syntax! {
        // 1. Scan part.
        part_filtered = source_iter(part)
        // 2. Pushed down filter on part.
            -> filter(|part| {
                Part::filter_1(&part.p_brand, &part.p_size, &part.p_container)
                || Part::filter_2(&part.p_brand, &part.p_size, &part.p_container)
                || Part::filter_3(&part.p_brand, &part.p_size, &part.p_container)
            });

        // 3. Scan lineitem.
        lineitem_filtered = source_iter(line_items)
            // 4. Pushed down lineitem filter.
            // l_shipinstruct = "DELIVER IN PERSON"
            // l_shipmode = "AIR" or "AIR REG"
            -> filter(|lineitem| lineitem.l_shipinstruct == "DELIVER IN PERSON" && (lineitem.l_shipmode == "AIR" || lineitem.l_shipmode == "AIR REG"))
            -> filter(|lineitem| LineItem::filter_1(&lineitem.l_quantity) || LineItem::filter_2(&lineitem.l_quantity) || LineItem::filter_3(&lineitem.l_quantity));

        // 5. Join the two
        // Build: Part?
        // Probe: LineItem?

        // Keys left (p_partkey)
        // Payload left (p_brand, p_container, p_size)
        part_filtered -> map(|p| (p.p_partkey, (p.p_brand, p.p_container, p.p_size))) -> [0]joined;
            /* .fold(HashMap::new(), |mut map, (key, value)| {
                map.insert(key, value);
                map
            }); */

        // Keys right (l_partkey)
        // Payload right (l_quantity, l_discount, l_extendedprice)
        lineitem_filtered -> map(|l| (l.l_partkey, (l.l_quantity, l.l_discount, l.l_extendedprice))) -> [1]joined;

        joined = join_multiset() -> map(|(_key, (part_payload, lineitem_payload))| {
            let (p_brand, p_container, p_size) = part_payload;
            let (l_quantity, l_discount, l_extendedprice) = lineitem_payload;
            (p_brand, p_container, p_size, l_quantity, l_discount, l_extendedprice)
        });

        // 6. Filter again, we need to make sure the right tuples survived.
        join_filtered = joined -> filter(|(p_brand, p_container, p_size, l_quantity, _l_discount, _l_extendedprice)| {
            (Part::filter_1(&p_brand, &p_size, &p_container) && LineItem::filter_1(&l_quantity))
            || (Part::filter_2(&p_brand, &p_size, &p_container) && LineItem::filter_2(&l_quantity))
            || (Part::filter_3(&p_brand, &p_size, &p_container) && LineItem::filter_3(&l_quantity))
        });

        // 7. Aggregate the result.
        // 7.1 Compute (l_extendedprice * (1 - l_discount))
        // 7.2. Aggregate sum
        agg = join_filtered -> map(|(_p_brand, _p_container, _p_size, _l_quantity, l_discount, l_extendedprice)| {
            l_extendedprice * (1.0 - l_discount)
        }) -> reduce(|a, b| *a += b);


        // 8. Print
        agg -> for_each(|x| {
            println!("{:?}", x);
        });
    };

    flow.run_available();
}

pub fn query_base(line_items: Vec<LineItem>, part: Vec<Part>) {

    let mut flow = hydroflow_syntax! {
        source_iter([(line_items, part)]) -> for_each(|(line_items, part)|{
            query_base_original(line_items, part);
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
        let limit = None;
        let conn = initialize_database(1);

        let line_items = LineItem::load(&conn, limit);
        let part = Part::load(&conn, limit);

        super::query(line_items, part);
    }
}