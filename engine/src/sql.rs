use anyhow::{anyhow, Result};
use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use ngdb_engine::{PersistentEventLog, Record};
use uuid::Uuid;

pub enum SqlResponse {
    Ack { offset: u64 },
    Rows(Vec<(u64, Vec<u8>)>),
}

pub async fn execute_sql(log: &PersistentEventLog, sql: &str) -> Result<SqlResponse> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    if ast.is_empty() {
        return Err(anyhow!("empty SQL"));
    }

    match &ast[0] {
        Statement::Insert { source: Some(source), .. } => {
            // Expect form: INSERT INTO table(key, value) VALUES (123, 'hex:...')
            let values = match &*source.body {
                SetExpr::Values(v) => v,
                _ => return Err(anyhow!("only VALUES supported")),
            };
            if values.rows.is_empty() {
                return Err(anyhow!("no values"));
            }
            let row = &values.rows[0];
            if row.len() < 2 {
                return Err(anyhow!("need key,value"));
            }
            let key = match &row[0] {
                Expr::Value(Value::Number(n, _)) => n.parse::<u64>()?,
                _ => return Err(anyhow!("key must be number")),
            };
            // Accept string literal; treat as raw bytes (UTF-8)
            let value = match &row[1] {
                Expr::Value(Value::SingleQuotedString(s)) => s.clone().into_bytes(),
                Expr::Value(Value::HexStringLiteral(h)) => hex::decode(h).unwrap_or_default(),
                _ => return Err(anyhow!("value must be string")),
            };
            let offset = log
                .append_kv(Uuid::new_v4(), key, value)
                .await?;
            Ok(SqlResponse::Ack { offset })
        }
        Statement::Query(q) => select_by_key(log, q).await,
        _ => Err(anyhow!("only INSERT and simple SELECT supported")),
    }
}

async fn select_by_key(log: &PersistentEventLog, q: &Box<Query>) -> Result<SqlResponse> {
    let body = &q.body;
    let SetExpr::Select(sel) = &**body else {
        return Err(anyhow!("only SELECT supported"));
    };

    let mut key_opt: Option<u64> = None;
    if let Some(selection) = &sel.selection {
        match selection {
            Expr::BinaryOp { left, op, right } if op.to_string() == "=" => {
                let (lhs, rhs) = (&**left, &**right);
                let num = match (lhs, rhs) {
                    (Expr::Identifier(_), Expr::Value(Value::Number(n, _))) => n.parse::<u64>()?,
                    (Expr::Value(Value::Number(n, _)), Expr::Identifier(_)) => n.parse::<u64>()?,
                    _ => return Err(anyhow!("WHERE key = <number> expected")),
                };
                key_opt = Some(num);
            }
            _ => {}
        }
    }

    let Some(key) = key_opt else { return Err(anyhow!("missing key in WHERE")); };
    if let Some(offset) = log.lookup_key(key).await {
        // Fetch the single event
        let events = log.replay(offset, Some(offset + 1)).await;
        if let Some(ev) = events.into_iter().next() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                return Ok(SqlResponse::Rows(vec![(rec.key, rec.value)]));
            }
        }
    }
    Ok(SqlResponse::Rows(Vec::new()))
}