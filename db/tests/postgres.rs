use db::{Column, DataType, Table, Value};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn insert_and_select() -> Result<(), Box<dyn std::error::Error>> {
    let node = Postgres::default().start().await?;

    let connection_string = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        node.get_host().await?,
        node.get_host_port_ipv4(5432).await?
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string)
        .await?;

    sqlx::query("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await?;

    let table = Table::new(
        "items",
        vec![
            Column {
                name: "id".into(),
                data_type: DataType::Int,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Text,
            },
        ],
    );

    let mut values = HashMap::new();
    values.insert("id".into(), Value::Int(1));
    values.insert("name".into(), Value::Text("hello".into()));

    table.insert(&pool, values).await?;

    let rows = table.select(&pool, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Text("hello".into())));

    Ok(())
}
