use chrono::{DateTime, NaiveDate};

// Conversion taken from DuckDB's main branch: https://github.com/duckdb/duckdb-rs/blob/a1aa55aff22b75e149e9cf7cface6464b3dc0ccc/src/types/chrono.rs#L71C39-L71C111
pub fn to_date(value: i32) -> NaiveDate {
    DateTime::from_timestamp(24 * 3600 * (value as i64), 0)
        .unwrap()
        .naive_utc()
        .date()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use duckdb::Connection;

    
    #[test]
    fn test_to_date() {
        let conn = Connection::open_in_memory().expect("Error creating in-memory database");
        let raw: i32 = conn
            .query_row("SELECT DATE '1992-03-22' as date;", [], |row| {
                row.get::<_, i32>(0)
            })
            .unwrap();
        let date = to_date(raw);
        assert_eq!(date, NaiveDate::from_ymd_opt(1992, 3, 22).unwrap());
    }
}