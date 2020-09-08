use bstr::BString;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Table {
    Binary(BString),
    Array(Vec<Value>),
    Map(HashMap<Value, Value>),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Value {
    Unit,
    Int(i64),
    Double(double),
    Enum(Tag, Value), // recursive. might need box or indirection.
    Struct(Vec<Value>),
    Nested(Table),
    Reversed(Value),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum LogicalMergeError {
    LogicalCannotMergeMismatchedCollections(Table, Table),
    LogicalCannotMergeMismatchedValues(Value, Value),
    LogicalCannotMergeInt(Int64, Int64),
    LogicalCannotMergeDouble(Double, Double),
    LogicalCannotMergeEnum((Tag, Value), (Tag, Value)),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum LogicalSchemaError {
    LogicalExpectedBinary(Table),
    LogicalExpectedArray(Table),
    LogicalExpectedMap(Table),
    LogicalExpectedInt(Value),
    LogicalExpectedDouble(Value),
    LogicalExpectedEnum(Value),
    LogicalExpectedStruct(Value),
    LogicalExpectedNested(Value),
    LogicalExpectedReversed(Value),
}

pub fn render_field<A>(name: String, x: A) -> String
where A: Display
{
    format!("\n\n {} ={}", name, pp_prefix("\n    ", x))
}

pub fn pp_prefix<A>(pefix: String, x: A) -> String
where A: Display
{
    format!("{}", x)
        .lines()
        .map(|line| format!("{}{}", prefix, line))
        .collect::<Vec<_>>()
        .join("")
}

impl Display for LogicalMergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogicalMergeError::LogicalCannotMergeMismatchedCollections(x, y),
                write!(f, "Cannot merge mismatched collections:{}{}",
                    render_field("first", x),
                    render_field("second", y)
                   ),
            LogicalMergeError::LogicalCannotMergeMismatchedValues(x, y)
                write!(f, "Cannot merge mismatched values:{}{}",
                    render_field("first", x),
                    render_field("second", y)
                   ),
            LogicalMergeError::LogicalCannotMergeInt(x, y)
                write!(f, "Cannot merge two integers:{}{}",
                    render_field("first", x),
                    render_field("second", y)
                   ),
            LogicalMergeError::LogicalCannotMergeDouble(x, y)
                write!(f, "Cannot merge two doubles:{}{}",
                    render_field("first", x),
                    render_field("second", y)
                   ),
            LogicalMergeError::LogicalCannotMergeEnum(x, y)
                write!(f, "Cannot merge two enums:{}{}",
                    render_field("first", x),
                    render_field("second", y)
                   ),
        }
    }
}

impl Display for LogicalMergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogicalExpectedBinary(x) =>
                write!(f, "Expected binary, but was: {}", pp_table_schema(x)),
            LogicalExpectedArray(x) =>
                write!(f, "Expected array, but was: {}", pp_table_schema(x)),
            LogicalExpectedMap(x) =>
                write!(f, "Expected map, but was: {}", pp_table_schema(x)),
            LogicalExpectedInt(x) =>
                write!(f, "Expected int, but was: {}", pp_column_schema(x)),
            LogicalExpectedDouble(x) =>
                write!(f, "Expected double, but was: {}", pp_column_schema(x)),
            LogicalExpectedEnum(x) =>
                write!(f, "Expected enum, but was: {}", pp_column_schema(x)),
            LogicalExpectedStruct(x) =>
                write!(f, "Expected struct, but was: {}", pp_column_schema(x)),
            LogicalExpectedNested(x) =>
                write!(f, "Expected nested, but was: {}", pp_column_schema(x)),
            LogicalExpectedReversed(x) =>
                write!(f, "Expected reversed, but was: {}", pp_column_schema(x)),
        }
    }
}

// TODO to_string
pub fn pp_table_schema(table: Table) -> String {
    match table {
        Table::Binary(_) => "binary".to_string(),
        Table::Array(_) => "array".to_string(),
        Table::Map(_) => "map".to_string(),
    }
}

// TODO to_string
pub fn pp_column_schema(v: Value) -> String {
    match v {
        Value::Unit => "unit".to_string(),
        Value::Int => "int".to_string(),
        Value::Double => "double".to_string(),
        Value::Enum => "enum".to_string(),
        Value::Struct => "struct".to_string(),
        Value::Nested => "nested".to_string(),
        Value::Reversed => "reversed".to_string(),
    }
}

// len
// this is a BigInt in Haskell (Int) but we do i64 here cus.
pub fn length(t: Table) -> i64 {
    match t {
        Table::Binary(bs) => bs.len(),
        Table::Array(xs) => xs.len(),
        Table::Map(kvs) => kvs.len(),
    }
}

pub fn size(t: Table) -> i64 {
    match t {
        Table::Binary(bs) => bs.len(),
        Table::Array(xs) => xs.iter().fold(0, |acc, x| size_value(x) + acc),
        Table::Map(kvs) => kvs.iter().fold(0, |acc, (k, v)| size_value(k) + size_value(v) + acc),

    }
}

pub fn size_value(v: Value) -> i64 {
    match v {
        Value::Unit => 8
        Value::Int => 8
        Value::Double => 8
        Value::Enum => 8 + size_value(x)
        Value::Struct(fields) => fields.iter().map(size_value).sum::<i64>(),
        Value::Nested(xs) => size(xs),
        Value::Reversed(xs) => size_value(xs),
    }
}

pub fn merge(x0: Table, x1: Table) -> Result<Table, LogicalMergeError> {
    match (x0, x1) {
        (Table::Binary(bs0), Table::Binary(bs1)) => Ok(Binary(bs0.concat(bs1))),
        (Table::Array(xs0), Table::Array(xs1)) => Ok(Array(xs0.concat(xs1))),
        (Table::Map(kvs0), Table::Map(kvs1)) => Ok(Map(merge_map(kvs0, kvs1))),
        _ => Err(LogicalCannotMergeMismatchedCollections(xs0, xs1))
    }
}

// https://hackage.haskell.org/package/containers-0.6.3.1/docs/Data-IntMap-Internal.html#v:mergeWithKey
pub fn merge_map(xs0: HashMap<Value, Value>, xs1: HashMap<Value, Value>) -> Result<HashMap<Value, Value>, LogicalMergeError> {
  //let
    //sequenceA' =
      //Map.traverseWithKey (const id)

  //in
    //sequenceA' $
      //Map.mergeWithKey (\_ x y -> Just (mergeValue x y)) (fmap pure) (fmap pure) xs0 xs1
}


pub fn merge_maps(kvss: Vec<HashMap<Value, Value>>) -> Result<HashMap<Value, Value>, LogicalMergeError> {
    match kvss.len() {
        0 => HashMap::new(),
        1 => kvss[0],
        2 => merge_map(kvss[0], kvss[1]),
        n => {
            let (kvss0, kvss1) = kvss.split_at(n / 2);
            let kvs0 = merge_maps(kvss0);
            let kvs1 = merge_maps(kvss1);
            merge_map(kvs0, kvs1)
        }
    }
}

pub fn merge_value(xs0: Value, xs1: Value) -> Result<Value, LogicalMergeError> {
    match (xs0, xs1) {
        (Value::Unit, Value::Unit) => Ok(Unit),
        (Value::Int(v0), Value::Int(v1)) => Err(LogicalCannotMergeInt(v0, v1)),
        (Value::Double(v0), Value::Double(v1)) => Err(LogicalCannotMergeDouble(v0, v1)),
        (Value::Enum(tag0, v0), Value::Enum(tag1, v1)) => Err(LogicalCannotMergeEnum((tag0, v0), (tag1, v1))), ,
        (Value::Struct(fs0), Value::Struct(fs1)) => Ok(Value::Struct(fs0.iter().zip(fs1).map(|(f0, f1)| merge_value(f0, f1)).collect()),
        (Value::Nested(xs0), Value::Nested(xs1)) => Ok(Value::Nested(merge(xs0, xs1))),
        (Value::Reversed(v1), Value::Reversed(v1)) => Ok(Value::Reversed(merge_value(v0, v1))),
        _ => Err(LogicalCannotMergeMismatchedValues(xs0, xs1)),
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct UnionStep {
    complete: HashMap<Value, Value>,
    remaining: Vec<HashMap<Value, Value>>,
}

pub fn union_step(key: Value, kvss: Vec<HashMap<Value, Value>>) -> Result<UnionStep, LogicalMergeError> {
  //let
    //(done0, done1, incomplete) =
      //Cons.unzip3 $ fmap (Map.splitLookup key) kvss

    //insert = \case
      //Nothing ->
        //id
      //Just x ->
        //Map.insert key x

    //dones =
      //Cons.zipWith insert done1 done0

  //done <- mergeMaps $ Cons.toVector dones

  //pure $ UnionStep done incomplete
}

pub fn empty(x: schema::Table) -> Table {
    match x {
        schema::Table::Binary(_, _) => Table::Binary(BString::new()),
        schema::Table::Array(_, _) => Table::Array(Vec::new()),
        schema::Table::Array(_, _, _) => Table::Map(HashMap::new()),
    }
}

pub fn default_table(x: schema::Table) -> Table {
    empty(x)
}

pub fn default_value(x: schema::Column) -> Value {
    match x {
        schema::Column::Unit => Value::Unit,
        schema::Column::Int => Value::Int(0),
        schema::Column::Double => Value::Double(0),
        schema::Column::Enum(_, vs) => Value::Enum(0, default_value(vs[0].data)),
        schema::Column::Struct(_, fs) => Value::Struct(fs.iter().map(|f| default_value(f.data))),
        schema::Column::Nested(s) => Value::Nested(default_table(s))
        schema::Column::Reversed(s) => Value::Reversed(default_table(s))
    }
}

// as_* or to_*
// method.
pub fn take_binary(x: Table) -> Result<BString, LogicalSchemaError> {
    if let Table::Binary(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedBinary(x))
    }
}

pub fn take_array(x: Table) -> Result<Vec<Value>, LogicalSchemaError> {
    if let Table::Array(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedArray(x))
    }
}

pub fn take_int(x: Value) -> Result<i64, LogicalSchemaError> {
    if let Value::Int(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedInt(x))
    }
}

pub fn take_double(x: Value) -> Result<f64, LogicalSchemaError> {
    if let Value::Double(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedDouble(x))
    }
}

pub fn take_enum(x: Value) -> Result<(Tag, Value), LogicalSchemaError> {
    if let Value::Enum(tag, x) = x {
        Ok((tag, x))
    } else {
        Err(LogicalExpectedEnum(x))
    }
}

pub fn take_struct(x: Value) -> Result<Vec<Value>, LogicalSchemaError> {
    if let Value::Struct(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedStruct(x))
    }
}

pub fn take_nested(x: Value) -> Result<Table, LogicalSchemaError> {
    if let Value::Nested(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedNested(x))
    }
}

pub fn take_reversed(x: Value) -> Result<Value, LogicalSchemaError> {
    if let Value::Reversed(x) = x {
        Ok(x)
    } else {
        Err(LogicalExpectedReversed(x))
    }
}
