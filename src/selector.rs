use anyhow::{Result, bail, ensure};
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum Selector {
    Bool(bool),
    Pred(String, String, Value),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}
impl Selector {
    pub fn parse(input: &str) -> Result<Self> {
        let tokens = tokenize(input)?;
        let mut p = Parser { tokens, pos: 0 };
        let result = p.or()?;
        ensure!(p.pos == p.tokens.len(), "unexpected selector token");
        Ok(result)
    }
    /// Peer discovery can only reject a selector when known fields prove it false.
    pub fn may_match(&self, v: &Value) -> bool {
        self.known(v) != Some(false)
    }
    fn known(&self, v: &Value) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            Self::Pred(k, _, _) => k
                .split('.')
                .try_fold(v, |v, k| v.get(k))
                .map(|_| self.matches(v)),
            Self::Not(a) => a.known(v).map(|b| !b),
            Self::And(a, b) => match (a.known(v), b.known(v)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Self::Or(a, b) => match (a.known(v), b.known(v)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }
    pub fn matches(&self, v: &Value) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::And(a, b) => a.matches(v) && b.matches(v),
            Self::Or(a, b) => a.matches(v) || b.matches(v),
            Self::Not(a) => !a.matches(v),
            Self::Pred(k, op, want) => {
                let value = k.split('.').try_fold(v, |v, k| v.get(k));
                let Some(value) = value else { return false };
                if let Some(a) = value.as_array() {
                    return match op.as_str() {
                        "=" | "==" => a.iter().any(|x| equal(x, want)),
                        "!=" => a.iter().all(|x| !equal(x, want)),
                        _ => false,
                    };
                }
                let ordering = match (number(value), number(want)) {
                    (Some(a), Some(b)) => a.partial_cmp(&b),
                    _ => Some(string(value).cmp(&string(want))),
                };
                match op.as_str() {
                    "=" | "==" => equal(value, want),
                    "!=" => !equal(value, want),
                    ">" => ordering.is_some_and(|x| x.is_gt()),
                    ">=" => ordering.is_some_and(|x| x.is_ge()),
                    "<" => ordering.is_some_and(|x| x.is_lt()),
                    "<=" => ordering.is_some_and(|x| x.is_le()),
                    _ => false,
                }
            }
        }
    }
}
fn number(v: &Value) -> Option<i64> {
    crate::tl::integer(v)
}
fn string(v: &Value) -> String {
    v.as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| v.to_string())
}
fn equal(a: &Value, b: &Value) -> bool {
    a == b || string(a) == string(b)
}
struct Parser {
    tokens: Vec<String>,
    pos: usize,
}
impl Parser {
    fn take(&mut self, t: &str) -> bool {
        if self.tokens.get(self.pos).is_some_and(|x| x == t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn next(&mut self) -> Result<String> {
        let v = self
            .tokens
            .get(self.pos)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("incomplete selector"))?;
        self.pos += 1;
        Ok(v)
    }
    fn or(&mut self) -> Result<Selector> {
        let mut v = self.and()?;
        while self.take("or") || self.take("||") {
            v = Selector::Or(Box::new(v), Box::new(self.and()?));
        }
        Ok(v)
    }
    fn and(&mut self) -> Result<Selector> {
        let mut v = self.atom()?;
        while self.take("and") || self.take("&&") {
            v = Selector::And(Box::new(v), Box::new(self.atom()?));
        }
        Ok(v)
    }
    fn atom(&mut self) -> Result<Selector> {
        if self.take("not") || self.take("!") {
            return Ok(Selector::Not(Box::new(self.atom()?)));
        }
        if self.take("(") {
            let v = self.or()?;
            ensure!(self.take(")"), "expected )");
            return Ok(v);
        }
        let name = self.next()?;
        if matches!(name.as_str(), "true" | "false") {
            return Ok(Selector::Bool(name == "true"));
        }
        if name == "mine" {
            return Ok(Selector::Pred(
                "outgoing".into(),
                "=".into(),
                Value::Bool(true),
            ));
        }
        if name == "personal" {
            return Ok(Selector::Pred(
                "category".into(),
                "=".into(),
                Value::String("personal".into()),
            ));
        }
        let op = self.next()?;
        ensure!(
            ["=", "==", "!=", ">", ">=", "<", "<="].contains(&op.as_str()),
            "invalid selector operator"
        );
        let literal = self.next()?;
        let value = serde_json::from_str(&literal).unwrap_or(Value::String(literal));
        Ok(Selector::Pred(name, op, value))
    }
}
fn tokenize(s: &str) -> Result<Vec<String>> {
    let mut result = vec![];
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '"' {
            let mut token = String::from('"');
            let mut escaped = false;
            let mut closed = false;
            for c in chars.by_ref() {
                token.push(c);
                if c == '"' && !escaped {
                    closed = true;
                    break;
                }
                escaped = c == '\\' && !escaped;
            }
            ensure!(closed, "unterminated selector string");
            result.push(token);
        } else if "()=!<>&|".contains(c) {
            let mut token = c.to_string();
            if chars
                .peek()
                .is_some_and(|n| *n == '=' || (*n == c && "&|".contains(c)))
            {
                token.push(chars.next().unwrap());
            }
            result.push(token);
        } else {
            let mut token = c.to_string();
            while chars
                .peek()
                .is_some_and(|n| !n.is_whitespace() && !"()=!<>&|".contains(*n))
            {
                token.push(chars.next().unwrap());
            }
            result.push(token);
        }
    }
    if result.is_empty() {
        bail!("empty selector");
    }
    Ok(result)
}
