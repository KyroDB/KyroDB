use anyhow::{anyhow, Result};
use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use ngdb_engine::{PersistentEventLog, Record};
use ngdb_engine::schema::{SchemaRegistry, TableKind, TableSchema};
use uuid::Uuid;

pub enum SqlResponse {
    Ack { offset: u64 },
    Rows(Vec<(u64, Vec<u8>)>),
    VecRows(Vec<(u64, f32)>),
}

fn parse_vector_from_expr(expr: &Expr) -> Option<Vec<f32>> {
    match expr {
        Expr::Array(arr) => {
            let mut out = Vec::with_capacity(arr.elem.len());
            for e in &arr.elem {
                if let Expr::Value(Value::Number(n, _)) = e {
                    if let Ok(v) = n.parse::<f32>() { out.push(v); }
                }
            }
            Some(out)
        }
        Expr::Tuple(ts) => {
            let mut out = Vec::new();
            for e in ts {
                if let Expr::Value(Value::Number(n, _)) = e {
                    if let Ok(v) = n.parse::<f32>() { out.push(v); }
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn extract_query_and_mode(expr: &Expr, out_query: &mut Option<Vec<f32>>, out_mode: &mut Option<String>) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let op_s = op.to_string();
            if op_s == "=" {
                // Handle Identifier on either side of '='
                let try_side = |ident_expr: &Expr, other: &Expr, out_query: &mut Option<Vec<f32>>, out_mode: &mut Option<String>| {
                    if let Expr::Identifier(id) = ident_expr {
                        let key = id.value.to_ascii_uppercase();
                        if key == "QUERY" {
                            if out_query.is_none() {
                                *out_query = parse_vector_from_expr(other);
                            }
                        } else if key == "MODE" {
                            if out_mode.is_none() {
                                if let Expr::Value(Value::SingleQuotedString(s)) = other {
                                    *out_mode = Some(s.to_ascii_uppercase());
                                }
                            }
                        }
                    }
                };
                try_side(&*left, &*right, out_query, out_mode);
                try_side(&*right, &*left, out_query, out_mode);
            } else {
                // Recurse across boolean operators like AND/OR
                extract_query_and_mode(left, out_query, out_mode);
                extract_query_and_mode(right, out_query, out_mode);
            }
        }
        _ => {}
    }
}

pub async fn execute_sql(log: &PersistentEventLog, sql: &str) -> Result<SqlResponse> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    if ast.is_empty() { return Err(anyhow!("empty SQL")); }

    match &ast[0] {
        Statement::CreateTable { name, columns, .. } => {
            // Explicit: CREATE VECTORS v (dim INT) → table name contains vectors and single column dim
            let tbl = name.to_string();
            let mut reg = SchemaRegistry::load(&log.registry_path());
            let lower = tbl.to_ascii_lowercase();
            if lower == "vectors" || lower.ends_with("_vectors") || lower.contains("vectors") {
                // Parse first column as dim if present, else error
                let dim = if let Some(col) = columns.first() { col.name.to_string().parse::<usize>().unwrap_or(0) } else { 0 };
                if dim == 0 { return Err(anyhow!("CREATE VECTORS requires dim INT as first column")); }
                reg.upsert_table(TableSchema { name: tbl, kind: TableKind::Vectors { dim } });
            } else {
                reg.upsert_table(TableSchema { name: tbl, kind: TableKind::Kv });
            }
            reg.save(&log.registry_path()).map_err(|e| anyhow!(e.to_string()))?;
            Ok(SqlResponse::Ack { offset: 0 })
        }
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
                    let reg = SchemaRegistry::load(&log.registry_path());
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

    // Support: WHERE QUERY = [...] [AND MODE='ANN']
    if let Some(selection) = &sel.selection {
        let mut query_vec: Option<Vec<f32>> = None;
        let mut mode: Option<String> = None;
        extract_query_and_mode(selection, &mut query_vec, &mut mode);
        if let Some(query) = query_vec {
            // Enforce vector dim
            let reg = SchemaRegistry::load(&log.registry_path());
            if let Some(dim) = reg.get_default_vectors_dim() { if dim != query.len() { return Err(anyhow!("vector dim mismatch")); } }
            let k = if let Some(lim) = &q.limit {
                if let Expr::Value(Value::Number(n, _)) = lim { n.parse::<usize>().unwrap_or(10) } else { 10 }
            } else { 10 };
            let use_ann = mode.as_deref() == Some("ANN");
            let res = if use_ann { log.search_vector_ann(&query, k).await } else { log.search_vector_l2(&query, k).await };
            return Ok(SqlResponse::VecRows(res));
        }
    }

    // Fallback: SELECT by key
    select_by_key(log, q).await
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
        let events = log.replay(offset, Some(offset + 1)).await;
        if let Some(ev) = events.into_iter().next() {
            if let Ok(rec) = bincode::deserialize::<Record>(&ev.payload) {
                return Ok(SqlResponse::Rows(vec![(rec.key, rec.value)]));
            }
        }
    }
    Ok(SqlResponse::Rows(Vec::new()))
}