use sqlx::{
    Row,
    postgres::{PgPool, PgRow},
    query::Query,
};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum DataType {
    Int,
    Text,
    Bool,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug)]
pub enum Condition {
    Eq(String, Value),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

impl Condition {
    fn build(&self, args: &mut Vec<Value>, idx: &mut i32) -> String {
        match self {
            Condition::Eq(col, val) => match val {
                Value::Null => format!("{} IS NULL", col),
                _ => {
                    *idx += 1;
                    args.push(val.clone());
                    format!("{} = ${}", col, idx)
                }
            },
            Condition::And(l, r) => {
                let lsql = l.build(args, idx);
                let rsql = r.build(args, idx);
                format!("({}) AND ({})", lsql, rsql)
            }
            Condition::Or(l, r) => {
                let lsql = l.build(args, idx);
                let rsql = r.build(args, idx);
                format!("({}) OR ({})", lsql, rsql)
            }
        }
    }
}

impl Table {
    pub fn new(name: &str, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    pub async fn insert(
        &self,
        pool: &PgPool,
        mut values: HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>, sqlx::Error> {
        let mut cols = Vec::new();
        let mut placeholders = Vec::new();
        let mut binds = Vec::new();
        let mut idx = 1;
        for c in &self.columns {
            if let Some(v) = values.remove(&c.name) {
                cols.push(c.name.clone());
                match v {
                    Value::Null => placeholders.push("NULL".to_string()),
                    _ => {
                        placeholders.push(format!("${}", idx));
                        idx += 1;
                        binds.push(v);
                    }
                }
            }
        }
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
            self.name,
            cols.join(", "),
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for b in &binds {
            query = bind(query, b);
        }
        let row = query.fetch_one(pool).await?;
        row_to_map(&row, &self.columns)
    }

    pub async fn select(
        &self,
        pool: &PgPool,
        condition: Option<Condition>,
    ) -> Result<Vec<HashMap<String, Value>>, sqlx::Error> {
        let mut args = Vec::new();
        let mut idx = 0;
        let mut sql = format!("SELECT * FROM {}", self.name);
        if let Some(cond) = condition {
            let cond_sql = cond.build(&mut args, &mut idx);
            sql.push_str(" WHERE ");
            sql.push_str(&cond_sql);
        }
        let mut query = sqlx::query(&sql);
        for a in &args {
            query = bind(query, a);
        }
        let rows = query.fetch_all(pool).await?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row_to_map(&row, &self.columns)?);
        }
        Ok(result)
    }
}

fn bind<'q>(
    q: Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: &Value,
) -> Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        Value::Int(i) => q.bind(*i),
        Value::Text(s) => q.bind(s.clone()),
        Value::Bool(b) => q.bind(*b),
        Value::Null => q.bind::<Option<i32>>(None),
    }
}

fn row_to_map(row: &PgRow, columns: &[Column]) -> Result<HashMap<String, Value>, sqlx::Error> {
    let mut map = HashMap::new();
    for col in columns {
        let val = match col.data_type {
            DataType::Int => {
                let v: Option<i64> = row.try_get(col.name.as_str())?;
                v.map(Value::Int).unwrap_or(Value::Null)
            }
            DataType::Text => {
                let v: Option<String> = row.try_get(col.name.as_str())?;
                v.map(Value::Text).unwrap_or(Value::Null)
            }
            DataType::Bool => {
                let v: Option<bool> = row.try_get(col.name.as_str())?;
                v.map(Value::Bool).unwrap_or(Value::Null)
            }
        };
        map.insert(col.name.clone(), val);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn compile_test() {
        let _ = Table::new(
            "dummy",
            vec![Column {
                name: "id".into(),
                data_type: DataType::Int,
            }],
        );
    }
}
