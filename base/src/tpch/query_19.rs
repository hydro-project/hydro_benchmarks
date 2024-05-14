use std::collections::HashMap;
use duckdb::Connection;

pub struct Part {
    pub p_partkey: i64,
    pub p_brand: String,
    pub p_container: String,
    pub p_size: i32,
}

impl Part {
    pub fn load(conn: &Connection, limit: Option<u32>) -> Vec<Self> {
        // Create iterator for Part
        let query = match limit {
            Some(limit) => format!(
                "SELECT p_partkey, p_brand, p_container, p_size FROM part LIMIT {};",
                limit
            ),
            None => "SELECT p_partkey, p_brand, p_container, p_size FROM part;".to_string(),
        };
        let mut stmt = conn
            .prepare(&query)
            .expect("Error preparing query for Part");
        let parts = stmt
            .query_map([], |row| {
                Ok(Part {
                    p_partkey: row.get(0)?,
                    p_brand: row.get(1)?,
                    p_container: row.get(2)?,
                    p_size: row.get(3)?,
                })
            })
            .expect("Error querying Part");

        let parts = parts.filter(|x| x.is_ok()).map(|x| x.unwrap());

        parts.collect()
    }

    fn filter(brand_pred: &str, brand_val: &str, size_between: &(i32, i32), size_val: &i32, container_list: &[&str], container_val: &str) -> bool {
        return brand_pred == brand_val
            && size_between.0 <= *size_val
            && *size_val <= size_between.1
            && container_list.contains(&container_val);
    }

    pub fn filter_1(brand_val: &str, size_val: &i32, container_val: &str) -> bool {
        return Part::filter("Brand#12", brand_val, &(1, 5), size_val, &["SM CASE", "SM BOX", "SM PACK", "SM PKG"], container_val);
    }

    pub fn filter_2(brand_val: &str, size_val: &i32, container_val: &str) -> bool {
        return Part::filter("Brand#23", brand_val, &(1, 10), size_val, &["MED BAG", "MED BOX", "MED PKG", "MED PACK"], container_val);
    }

    pub fn filter_3(brand_val: &str, size_val: &i32, container_val: &str) -> bool {
        return Part::filter("Brand#34", brand_val, &(1, 15), size_val, &["LG CASE", "LG BOX", "LG PACK", "LG PKG"], container_val);
    }
}

pub struct LineItem {
    pub l_partkey: i64,
    pub l_shipmode: String,
    pub l_quantity: f64,
    pub l_shipinstruct: String,
    pub l_discount: f64,
    pub l_extendedprice: f64,
}

impl LineItem {
    pub fn load(conn: &Connection, limit: Option<u32>) -> Vec<Self> {
        // Create iterator for LineItem
        let query = match limit {
            Some(limit) => format!(
                "SELECT l_partkey, l_shipmode, l_quantity, l_shipinstruct, l_discount, l_extendedprice FROM lineitem LIMIT {};",
                limit
            ),
            None => "SELECT l_partkey, l_shipmode, l_quantity, l_shipinstruct, l_discount, l_extendedprice FROM lineitem;".to_string(),
        };
        let mut stmt = conn
            .prepare(&query)
            .expect("Error preparing query for LineItem");
        let line_items = stmt
            .query_map([], |row| {
                Ok(LineItem {
                    l_partkey: row.get(0)?,
                    l_shipmode: row.get(1)?,
                    l_quantity: row.get(2)?,
                    l_shipinstruct: row.get(3)?,
                    l_discount: row.get(4)?,
                    l_extendedprice: row.get(5)?,
                })
            })
            .expect("Error querying LineItem");

        let line_items = line_items.filter(|x| x.is_ok()).map(|x| x.unwrap());

        line_items.collect()
    }

    fn filter(quantity_between: &(f64, f64), quantity_val: &f64) -> bool {
        return quantity_between.0 <= *quantity_val && *quantity_val <= quantity_between.1;
    }

    pub fn filter_1(quantity_val: &f64) -> bool {
        return LineItem::filter(&(1.0, 11.0), quantity_val);
    }

    pub fn filter_2(quantity_val: &f64) -> bool {
        return LineItem::filter(&(10.0, 20.0), quantity_val);
    }

    pub fn filter_3(quantity_val: &f64) -> bool {
        return LineItem::filter(&(20.0, 30.0), quantity_val);
    }
}

pub fn load(conn: &Connection) -> (Vec<LineItem>, Vec<Part>) {
    (LineItem::load(conn, None), Part::load(conn, None))
}

pub fn query(line_items: Vec<LineItem>, part: Vec<Part>) {
    // XXX: InkFuse adds an early filter before the join. DuckDB does not
    
    // 1. Scan part.
    let part_filtered = part
        .into_iter()
    // 2. Pushed down filter on part.
        .filter(|part| {
            Part::filter_1(&part.p_brand, &part.p_size, &part.p_container)
            || Part::filter_2(&part.p_brand, &part.p_size, &part.p_container)
            || Part::filter_3(&part.p_brand, &part.p_size, &part.p_container)
        });

    // 3. Scan lineitem.
    let lineitem_filtered = line_items.into_iter()
        // 4. Pushed down lineitem filter.
        // l_shipinstruct = "DELIVER IN PERSON"
        // l_shipmode = "AIR" or "AIR REG"
        .filter(|lineitem| lineitem.l_shipinstruct == "DELIVER IN PERSON" && (lineitem.l_shipmode == "AIR" || lineitem.l_shipmode == "AIR REG"))
        .filter(|lineitem| LineItem::filter_1(&lineitem.l_quantity) || LineItem::filter_2(&lineitem.l_quantity) || LineItem::filter_3(&lineitem.l_quantity));
    
    // 5. Join the two
    // Build: Part?
    // Probe: LineItem?

    // Keys left (p_partkey)
    // Payload left (p_brand, p_container, p_size)
    let join_build = part_filtered.map(|p| (p.p_partkey, (p.p_brand, p.p_container, p.p_size)))
        .fold(HashMap::new(), |mut map, (key, value)| {
            map.insert(key, value);
            map
        });

    // Keys right (l_partkey)
    // Payload right (l_quantity, l_discount, l_extendedprice)
    let join_probe = lineitem_filtered.map(|l| (l.l_partkey, (l.l_quantity, l.l_discount, l.l_extendedprice)));

    let join = join_probe.filter_map(|(key, value)| {
        if let Some((p_brand, p_container, p_size)) = join_build.get(&key) {
            Some((p_brand, p_container, p_size, value.0, value.1, value.2))
        } else {
            None
        }
    });

    // 6. Filter again, we need to make sure the right tuples survived.
    let join_filtered = join.filter(|(p_brand, p_container, p_size, l_quantity, _l_discount, _l_extendedprice)| {
        (Part::filter_1(&p_brand, &p_size, &p_container) && LineItem::filter_1(&l_quantity))
        || (Part::filter_2(&p_brand, &p_size, &p_container) && LineItem::filter_2(&l_quantity))
        || (Part::filter_3(&p_brand, &p_size, &p_container) && LineItem::filter_3(&l_quantity))
    });

    // 7. Aggregate the result.
    // 7.1 Compute (l_extendedprice * (1 - l_discount))
    // 7.2. Aggregate sum
    let agg = join_filtered.map(|(_p_brand, _p_container, _p_size, _l_quantity, l_discount, l_extendedprice)| {
        l_extendedprice * (1.0 - l_discount)
    }).reduce(|a, b| a + b);


    // 8. Print
    println!("{}", agg.unwrap());

}

pub fn query_duckdb(conn: &Connection, limit: Option<u32>) {
    let lineitem_table = match limit {
        Some(limit) => format!("(SELECT * FROM lineitem LIMIT {})", limit),
        None => "lineitem".to_string(),
    };
    let part_table = match limit {
        Some(limit) => format!("(SELECT * FROM part LIMIT {})", limit),
        None => "part".to_string(),
    };
    let mut stmt = conn
        .prepare(&format!(
            r#"
            SELECT
            sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
            {},
            {}
        WHERE (p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1
            AND l_quantity <= 1 + 10
            AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10
                AND l_quantity <= 10 + 10
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey
                AND p_brand = 'Brand#34'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20
                AND l_quantity <= 20 + 10
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON');
    "#,
            lineitem_table, part_table
        ))
        .expect("Error preparing query for LineItem");
    let mut rows = stmt.query([]).expect("Error executing Query 1");
    while let Some(row) = rows.next().unwrap() {
        let revenue: f64 = row.get(0).unwrap();
        println!("{}", revenue);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tpch::initialize::initialize_database;

    #[test]
    fn test_query_19() {
        let limit = None;
        let conn = initialize_database(1);
        let line_items = LineItem::load(&conn, limit);
        let parts = Part::load(&conn, limit);
        query(line_items, parts);
    }

    #[test]
    fn test_query_19_duckdb() {
        let conn = initialize_database(1);
        query_duckdb(&conn, None);
    }
}