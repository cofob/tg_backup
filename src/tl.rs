//! Schema-driven decoding preserves old Telegram layers without a second storage format.
use anyhow::{Context, Result, bail, ensure};
use grammers_tl_parser::{
    parse_tl_file,
    tl::{Definition, ParameterType, Type},
};
use grammers_tl_types::{Cursor, Deserializable, Serializable};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

pub const API_SCHEMA: &str = include_str!("../schemas/telegram-layer-227.tl");
pub const LAYER: i32 = grammers_tl_types::LAYER;

pub struct Schema {
    definitions: HashMap<u32, Definition>,
    names: HashMap<String, u32>,
}
#[derive(Debug, Clone)]
pub struct ObjectSlice {
    pub root: String,
    pub offset: usize,
    pub length: usize,
    pub value: Value,
}
impl Schema {
    pub fn parse(text: &str) -> Result<Self> {
        let mut definitions = HashMap::new();
        let mut names = HashMap::new();
        for def in parse_tl_file(text) {
            let def = match def {
                Ok(d) => d,
                Err(grammers_tl_parser::errors::ParseError::NotImplemented) => continue,
                Err(e) => bail!("TL schema: {e:?}"),
            };
            names.insert(qualified(&def.namespace, &def.name), def.id);
            definitions.insert(def.id, def);
        }
        ensure!(!definitions.is_empty(), "empty TL schema");
        Ok(Self { definitions, names })
    }
    pub fn current() -> Result<Self> {
        Self::parse(API_SCHEMA)
    }
    pub fn decode(&self, root: &str, bytes: &[u8]) -> Result<Value> {
        Ok(self.decode_slices(root, bytes)?.0)
    }
    pub fn decode_slices(&self, root: &str, bytes: &[u8]) -> Result<(Value, Vec<ObjectSlice>)> {
        let mut cursor = Cursor::from_slice(bytes);
        let mut slices = vec![];
        let value = self.read(
            &root.parse().map_err(|e| anyhow::anyhow!("{e:?}"))?,
            &mut cursor,
            &mut slices,
            0,
        )?;
        ensure!(cursor.pos() == bytes.len(), "trailing bytes in TL {root}");
        Ok((value, slices))
    }
    fn read(
        &self,
        ty: &Type,
        cursor: &mut Cursor<'_>,
        slices: &mut Vec<ObjectSlice>,
        depth: usize,
    ) -> Result<Value> {
        ensure!(depth < 128, "TL nesting limit");
        Ok(match ty.name.as_str() {
            "int" => json!(i32::deserialize(cursor)?),
            "long" => json!(i64::deserialize(cursor)?.to_string()),
            "double" => {
                let v = f64::deserialize(cursor)?;
                ensure!(v.is_finite(), "non-finite TL double");
                json!(v)
            }
            "string" | "bytes" => {
                // Use the generated primitive codec. Bytes use an explicit hex wrapper in JSON.
                let b = Vec::<u8>::deserialize(cursor)?;
                if ty.name == "string" {
                    match String::from_utf8(b) {
                        Ok(s) => json!(s),
                        Err(e) => json!({"$bytes":hex::encode(e.into_bytes())}),
                    }
                } else {
                    json!({"$bytes":hex::encode(b)})
                }
            }
            "int128" => json!({"$bytes":hex::encode(<[u8;16]>::deserialize(cursor)?)}),
            "int256" => json!({"$bytes":hex::encode(<[u8;32]>::deserialize(cursor)?)}),
            "true" => json!(true),
            "Vector" | "vector" => {
                if !ty.bare {
                    ensure!(u32::deserialize(cursor)? == 0x1cb5c415, "invalid TL vector");
                }
                let n = i32::deserialize(cursor)?;
                ensure!((0..=1_000_000).contains(&n), "invalid TL vector length");
                let item = ty
                    .generic_arg
                    .as_ref()
                    .context("vector element type missing")?;
                let mut values = Vec::new();
                for _ in 0..n {
                    values.push(self.read(item, cursor, slices, depth + 1)?);
                }
                Value::Array(values)
            }
            _ => {
                let start = cursor.pos();
                let id = if ty.bare {
                    *self
                        .names
                        .get(&ty.to_string())
                        .context("unknown bare TL constructor")?
                } else {
                    u32::deserialize(cursor)?
                };
                let def = self
                    .definitions
                    .get(&id)
                    .with_context(|| format!("unknown TL constructor {id:08x}"))?;
                ensure!(
                    ty.bare || ty.name == "Object" || def.ty.to_string() == ty.to_string(),
                    "TL constructor type mismatch: expected {ty}, got {}",
                    def.ty
                );
                let mut object = Map::new();
                object.insert("_".into(), json!(qualified(&def.namespace, &def.name)));
                let mut flags = HashMap::new();
                for p in &def.params {
                    match &p.ty {
                        ParameterType::Flags => {
                            let f = u32::deserialize(cursor)?;
                            flags.insert(p.name.clone(), f);
                            object.insert(p.name.clone(), json!(f));
                        }
                        ParameterType::Normal { ty, flag } => {
                            if flag.as_ref().is_some_and(|f| {
                                flags.get(&f.name).copied().unwrap_or(0) & (1 << f.index) == 0
                            }) {
                                continue;
                            }
                            object
                                .insert(p.name.clone(), self.read(ty, cursor, slices, depth + 1)?);
                        }
                    }
                }
                let value = Value::Object(object);
                slices.push(ObjectSlice {
                    root: if ty.bare {
                        ty.to_string()
                    } else {
                        def.ty.to_string()
                    },
                    offset: start,
                    length: cursor.pos() - start,
                    value: value.clone(),
                });
                value
            }
        })
    }
    pub fn encode(&self, root: &str, value: &Value) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        self.write(
            &root.parse().map_err(|e| anyhow::anyhow!("{e:?}"))?,
            value,
            &mut bytes,
            0,
        )?;
        Ok(bytes)
    }
    fn write(&self, ty: &Type, value: &Value, out: &mut Vec<u8>, depth: usize) -> Result<()> {
        ensure!(depth < 128, "TL nesting limit");
        match ty.name.as_str() {
            "int" => i32::try_from(integer(value).context("expected TL int")?)?.serialize(out),
            "long" => integer(value).context("expected TL long")?.serialize(out),
            "double" => value.as_f64().context("expected TL double")?.serialize(out),
            "true" => ensure!(value == &json!(true), "expected TL true"),
            "string" | "bytes" | "int128" | "int256" => {
                let bytes = if let Some(hex) = value.get("$bytes").and_then(Value::as_str) {
                    hex::decode(hex)?
                } else {
                    value
                        .as_str()
                        .context("expected TL string/bytes")?
                        .as_bytes()
                        .to_vec()
                };
                if ty.name == "int128" || ty.name == "int256" {
                    ensure!(
                        bytes.len() == if ty.name == "int128" { 16 } else { 32 },
                        "wrong integer width"
                    );
                    out.extend(bytes);
                } else {
                    bytes.serialize(out);
                }
            }
            "Vector" | "vector" => {
                let a = value.as_array().context("expected TL vector")?;
                if !ty.bare {
                    0x1cb5c415u32.serialize(out);
                }
                i32::try_from(a.len())?.serialize(out);
                for v in a {
                    self.write(
                        ty.generic_arg.as_ref().context("missing vector type")?,
                        v,
                        out,
                        depth + 1,
                    )?;
                }
            }
            _ => {
                let name = if ty.bare {
                    ty.to_string()
                } else {
                    value["_"]
                        .as_str()
                        .context("TL constructor missing")?
                        .into()
                };
                let def = self
                    .definitions
                    .get(
                        self.names
                            .get(&name)
                            .context("unknown TL constructor name")?,
                    )
                    .unwrap();
                ensure!(
                    ty.bare || ty.name == "Object" || def.ty.to_string() == ty.to_string(),
                    "TL type mismatch"
                );
                if !ty.bare {
                    def.id.serialize(out);
                }
                // Preserve flags with no known fields, recompute all schema-known bits.
                let mut flags: HashMap<String, u32> = HashMap::new();
                for p in &def.params {
                    if matches!(p.ty, ParameterType::Flags) {
                        flags.insert(p.name.clone(), value[&p.name].as_u64().unwrap_or(0) as u32);
                    }
                }
                let mut groups: HashMap<(String, usize), Vec<bool>> = HashMap::new();
                for p in &def.params {
                    if let ParameterType::Normal { flag: Some(f), .. } = &p.ty {
                        groups.entry((f.name.clone(), f.index)).or_default().push(
                            value
                                .get(&p.name)
                                .is_some_and(|v| !v.is_null() && v != &json!(false)),
                        );
                    }
                }
                for ((name, bit), present) in groups {
                    ensure!(
                        present.iter().all(|v| *v == present[0]),
                        "shared TL flag fields must be present together"
                    );
                    let f = flags.get_mut(&name).context("unknown flag")?;
                    *f &= !(1 << bit);
                    if present[0] {
                        *f |= 1 << bit;
                    }
                }
                for p in &def.params {
                    match &p.ty {
                        ParameterType::Flags => flags[&p.name].serialize(out),
                        ParameterType::Normal { ty, flag } => {
                            if flag
                                .as_ref()
                                .is_some_and(|f| flags[&f.name] & (1 << f.index) == 0)
                            {
                                continue;
                            }
                            self.write(
                                ty,
                                value
                                    .get(&p.name)
                                    .with_context(|| format!("missing {name}.{}", p.name))?,
                                out,
                                depth + 1,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub fn request(&self, name: &str, args: Value) -> Result<(Vec<u8>, String)> {
        let def = self
            .definitions
            .get(self.names.get(name).context("unknown RPC")?)
            .unwrap();
        let mut data = def.id.to_le_bytes().to_vec();
        data.extend(self.encode(name, &args)?);
        Ok((data, def.ty.to_string()))
    }
    pub fn redact(&self, value: &mut Value, fields: &[String]) -> Result<usize> {
        let mut count = 0;
        match value {
            Value::Object(obj) => {
                if let Some(name) = obj.get("_").and_then(Value::as_str).map(str::to_owned) {
                    let def = self
                        .definitions
                        .get(self.names.get(&name).context("unknown constructor")?)
                        .unwrap();
                    for field in fields {
                        if let Some(field) = field.strip_prefix(&format!("{name}.")) {
                            let p = def
                                .params
                                .iter()
                                .find(|p| p.name == field)
                                .context("unknown retention field")?;
                            ensure!(
                                !matches!(
                                    field,
                                    "id" | "peer_id"
                                        | "from_id"
                                        | "user_id"
                                        | "chat_id"
                                        | "channel_id"
                                ),
                                "structural identity cannot be removed"
                            );
                            ensure!(
                                matches!(p.ty, ParameterType::Normal { flag: Some(_), .. }),
                                "required TL field cannot be removed: {name}.{field}"
                            );
                            count += usize::from(obj.remove(field).is_some());
                        }
                    }
                }
                for v in obj.values_mut() {
                    count += self.redact(v, fields)?;
                }
            }
            Value::Array(a) => {
                for v in a {
                    count += self.redact(v, fields)?;
                }
            }
            _ => {}
        }
        Ok(count)
    }
}
fn qualified(ns: &[String], name: &str) -> String {
    if ns.is_empty() {
        name.into()
    } else {
        format!("{}.{}", ns.join("."), name)
    }
}
pub fn integer(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}
pub use tg_backup_protocol::text;

pub struct RawRequest(pub Vec<u8>);
pub struct RawResponse(pub Vec<u8>);
impl Serializable for RawRequest {
    fn serialize(&self, buf: &mut impl Extend<u8>) {
        buf.extend(self.0.iter().copied());
    }
}
impl grammers_tl_types::RemoteCall for RawRequest {
    type Return = RawResponse;
}
impl Deserializable for RawResponse {
    fn deserialize(buf: &mut Cursor<'_>) -> grammers_tl_types::deserialize::Result<Self> {
        let mut bytes = vec![];
        buf.read_to_end(&mut bytes)?;
        Ok(Self(bytes))
    }
}
