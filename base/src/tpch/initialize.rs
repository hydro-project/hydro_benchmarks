use duckdb::Connection;

/**
 * Using DuckDB to load the data
 * */ 
pub fn initialize_database(scale_factor: u32) -> Connection {

    // Create a in-memory database
    let conn = Connection::open_in_memory().expect("Error creating in-memory database");
    conn.execute(
        &format!("CREATE OR REPLACE SCHEMA SF_{};", scale_factor),
        [],
    )
    .expect("Error creating schema");
    conn.execute(&format!("USE SF_{};", scale_factor), [])
        .expect("Error using schema");
    // Load the data via TPCH extension
    conn.execute(&format!("CALL dbgen(sf ={});", scale_factor), [])
        .expect("Error loading data via TPCH extension");

    conn
}