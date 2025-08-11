use anyhow::{anyhow, Result};
use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use ngdb_engine::{PersistentEventLog, Record};
use uuid::Uuid;

pub enum SqlResponse {
    Ack { offset: u64 },
    Rows(Vec<(u64, Vec<u8>)>),
    VecRows(Vec<(u64, f32)>),
}

pub async fn execute_sql(log: &PersistentEventLog, sql: &str) -> Result<SqlResponse> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    if ast.is_empty() { return Err(anyhow!("empty SQL")); }

    match &ast[0] {
        Statement::CreateTable { name, columns, .. } => {
            // Heuristic: if table name ends with _vec or vector keyword appears, treat as vectors with dim from column count-1
            let tbl = name.to_string();
            let mut reg = crate::schema::SchemaRegistry::load(&log.registry_path());
            if tbl.to_ascii_lowercase().contains("vector") || tbl.ends_with("_vec") {
                // Expect last column like dim
                let dim = columns.len().saturating_sub(1).max(1);
                reg.upsert_table(crate::schema::TableSchema { name: tbl, kind: crate::schema::TableKind::Vectors { dim } });
            } else {
                reg.upsert_table(crate::schema::TableSchema { name: tbl, kind: crate::schema::TableKind::Kv });
            }
            reg.save(&log.registry_path()).map_err(|e| anyhow!(e))?;
            Ok(SqlResponse::Ack { offset: 0 })
        }
        Statement::CreateView { .. } => Err(anyhow!("views not supported")),
        Statement::Insert { source: Some(source), .. } => {
            let values = match &*source.body { SetExpr::Values(v) => v, _ => return Err(anyhow!("only VALUES supported")) };
            if values.rows.is_empty() { return Err(anyhow!("no values")); }
            let row = &values.rows[0];
            if row.len() < 2 { return Err(anyhow!("need key,value")); }

            let key = match &row[0] { Expr::Value(Value::Number(n, _)) => n.parse::<u64>()?, _ => return Err(anyhow!("key must be number")), };

            match &row[1] {
                Expr::Value(Value::SingleQuotedString(s)) => {
                    let offset = log.append_kv(Uuid::new_v4(), key, s.clone().into_bytes()).await?;
                    Ok(SqlResponse::Ack { offset })
                }
                Expr::Array(arr) => {
                    let mut vec: Vec<f32> = Vec::with_capacity(arr.elem.len());
                    for e in &arr.elem { if let Expr::Value(Value::Number(n, _)) = e { vec.push(n.parse::<f32>()?); } }
                    // Enforce dimension if registry has one
                    let reg = crate::schema::SchemaRegistry::load(&log.registry_path());
                    if let Some(dim) = reg.get_default_vectors_dim() { if dim != vec.len() { return Err(anyhow!("vector dim mismatch")); } }
                    let offset = log.append_vector(Uuid::new_v4(), key, vec).await?;
                    Ok(SqlResponse::Ack { offset })
                }
                _ => Err(anyhow!("unsupported value type")),
            }
        }
        Statement::Query(q) => select_query(log, q).await,
        _ => Err(anyhow!("only INSERT and simple SELECT supported")),
    }
}

async fn select_query(log: &PersistentEventLog, q: &Box<Query>) -> Result<SqlResponse> {
    let body = &q.body;
    let SetExpr::Select(sel) = &**body else { return Err(anyhow!("only SELECT supported")); };

    if let Some(selection) = &sel.selection {
        if let Expr::BinaryOp { left, op, right } = selection {
            if op.to_string() == "=" {
                if let (Expr::Identifier(ident), expr) = (&**left, &**right) {
                    if ident.value.to_ascii_uppercase() == "QUERY" {
                        let mut query: Vec<f32> = Vec::new();
                        match expr {
                            Expr::Array(arr) => {
                                for e in &arr.elem { if let Expr::Value(Value::Number(n, _)) = e { query.push(n.parse::<f32>()?); } }
                            }
                            Expr::Tuple(ts) => {
                                for e in ts { if let Expr::Value(Value::Number(n, _)) = e { query.push(n.parse::<f32>()?); } }
                            }
                            _ => {}
                        }
                        // Enforce vector dim
                        let reg = crate::schema::SchemaRegistry::load(&log.registry_path());
                        if let Some(dim) = reg.get_default_vectors_dim() { if dim != query.len() { return Err(anyhow!("vector dim mismatch")); } }
                        let k = if let Some(lim) = &q.limit {
                            if let Expr::Value(Value::Number(n, _)) = lim { n.parse::<usize>().unwrap_or(10) } else { 10 }
                        } else { 10 };
                        let res = log.search_vector_l2(&query, k).await;
                        return Ok(SqlResponse::VecRows(res));
                    }
                }
            }
        }
    }

    select_by_key(log, q).await
}

async fn select_by_key(log: &PersistentEventLog, q: &Box<Query>) -> Result<SqlResponse> {
    let body = &q.body;
    let SetExpr::Select(sel) = &**body else { return Err(anyhow!("only SELECT supported")); };

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
        let events = log.replay(offset, Some(offset + 1)).await;
        if let Some(ev) = events.into_iter().next() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                return Ok(SqlResponse::Rows(vec![(rec.key, rec.value)]));
            }
        }
    }
    Ok(SqlResponse::Rows(Vec::new()))
}