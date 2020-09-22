#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Table {
    Binary(Default, encoding::Table::Binary),
    Array(Default, Column),
    Map(Default, Column, Column),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Table {
    Unit,
    Int(Default, encoding::Value::Int),
    Double(Default, encoding::Value::Double),
    Enum(Default, Vec<Variant<Column>>),
    Struct(Default, Vec<Field<Column>>),
    Nested(Table),
    Reversed(Column),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum SchemaError {
    SchemaExpectedBinary(Table),
    SchemaExpectedArray(Table),
    SchemaExpectedMap(Table),
    SchemaExpectedInt(Column),
    SchemaExpectedDouble(Column),
    SchemaExpectedEnum(Column),
    SchemaExpectedStruct(Column),
    SchemaExpectedNested(Column),
    SchemaExpectedReversed(Column),
    SchemaExpectedOption((Vec<(Variant<Column>)>)),
    SchemaExpectedEither((Vec<(Variant<Column>)>)),
    SchemaExpectedPair((Vec<(Field<Column>)>)),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum SchemaUnionError {
  SchemaUnionMapKeyNotAllowed(Table, Table),
  SchemaUnionDefaultNotAllowed((Field Column)),
  SchemaUnionFailedLookupInternalError((Field Column)),
  SchemaUnionTableMismatch(Table, Table),
  SchemaUnionColumnMismatch(Column, Column),
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaError::SchemaExpectedBinary(x) =>
                write!(f, "Expected Binary, but was: {}", x),
            SchemaError::SchemaExpectedArray(x) =>
                write!(f, "Expected Array, but was: {}", x),
            SchemaError::SchemaExpectedMap(x) =>
                write!(f, "Expected Map, but was: {}", x),
            SchemaError::SchemaExpectedInt(x) =>
                write!(f, "Expected Int, but was: {}", x),
            SchemaError::SchemaExpectedDouble(x) =>
                write!(f, "Expected Double, but was: {}", x),
            SchemaError::SchemaExpectedEnum(x) =>
                write!(f, "Expected Enum, but was: {}", x),
            SchemaError::SchemaExpectedStruct(x) =>
                write!(f, "Expected Struct, but was: {}", x),
            SchemaError::SchemaExpectedNested(x) =>
                write!(f, "Expected Nested, but was: {}", x),
            SchemaError::SchemaExpectedReversed(x) =>
                write!(f, "Expected Reversed, but was: {}", x),
            SchemaError::SchemaExpectedOption(x) =>
                write!(f, "Expected Option, but was: {}", x),
            SchemaError::SchemaExpectedEither(x) =>
                write!(f, "Expected Either, but was: {}", x),
            SchemaError::SchemaExpectedPair(x) =>
                write!(f, "Expected Pair, but was: {}", x),
        }
    }
}

impl Display for SchemaUnionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        SchemaUnionError::SchemaUnionMapKeyNotAllowed(x, y) =>
            write!(f, "Cannot union tables with different map keys, it could invalidate the ordering invariant:{} {}", pp_field("first", x), pp_field("second", y)),
        SchemaUnionError::SchemaUnionDefaultNotAllowed(Field(name, value)) =>
            write!(f, "Schema did not allow defaulting of struct field:{}", pp_field(name.name, value)),
        SchemaUnionError::SchemaUnionFailedLookupInternalError(Field(name, value)) =>
            write!(f, "This should not have happened, please report an issue. Internal error when trying to union struct field:{}", pp_field(name.name, value)),
        SchemaUnionError::SchemaUnionTableMismatch(x, y) =>
            write!(f, "Cannot union tables with incompatible schemas:{}{}", pp_field("first", x), pp_field("second", y)),
        SchemaUnionError::SchemaUnionColumnMismatch(x, y) =>
            write!(f, "Cannot union columns with incompatible schemas:{}{}", pp_field("first", x), pp_field("second", y)),
    }
}

fn pp_field<A: Debug>(name: String, x: A) -> String {
    format!("\n\n {} ={}", pp_prefix("\n    ", x))
}

fn pp_prefix<A: Debug>(prefix: String, x: A) -> String {
    format!(format!("{}", x).lines().into_iter().map(|l| format!("{}{}", prefix, l)).collect::Vec<String>().join(""))
}

fn false() -> Variant<Column> {
    Variant("false", Unit)
}

fn true() -> Variant<Column> {
    Variant("true", Unit)
}

fn bool(def: Default) -> Column {
    Enum(def, vec![false, true])
}

fn none() -> Variant<Column> {
    Variant("none", Unit)
}

fn some(c: Column) -> Variant<Column> {
    Variant("some", c)
}

fn option(def: Default, c: Column) -> Column {
    Enum(def, vec![none, some]);
}

fn left(c: Column) -> Variant<Column> {
    Variant("left", c)
}

fn right(c: Column) -> Variant<Column> {
    Variant("right", c)
}

fn either(def: Default, l: Column, r: Column) -> Column {
    Enum(def, vec![l, r]);
}

fn first(c: column) -> Field<Column> {
    Field("first", c)
}

fn second(c: Column) -> Field<Column> {
    Field("second", c)
}

fn pair(def: Default, x: Column, y: Column) -> Column {
    Struct(def, vec![first(x), second(y)])
}

pub fn take_binary(t: Table) -> Result<(Default, encoding::Binary), SchemaError> {
    match t {
        Table::Binary(def, encoding) => Ok((def, encoding)),
        x => Err(SchemaExpectedBinary(x)),
    }
}

pub fn take_array(t: Table) -> Result<(Default, Column), SchemaError> {
    match t {
        Table::Array(def, x) => Ok((def, x)),
        x => Err(SchemaExpectedArray(x)),
    }
}

pub fn take_map(t: Table) -> Result<(Default, Column, Column), SchemaError> {
    match t {
        Table::Map(def, k, v) => Ok((def, x, v)),
        x => Err(SchemaExpectedMap(x)),
    }
}

pub fn take_int(t: Column) -> Result<(Default, encoding::Int), SchemaError> {
    match t {
        Table::Int(def, encoding) => Ok((def, encoding)),
        x => Err(SchemaExpectedInt(x)),
    }
}

pub fn take_double(t: Column) -> Result<Default, SchemaError> {
    match t {
        Table::Int(def) => Ok(def),
        x => Err(SchemaExpectedDouble(x)),
    }
}

pub fn take_double(t: Column) -> Result<Default, SchemaError> {
    match t {
        Table::Int(def) => Ok(def),
        x => Err(SchemaExpectedDouble(x)),
    }
}

pub fn take_enum(t: Column) -> Result<(Default, Vec<Variant<Column>>), SchemaError> {
    match t {
        Table::Enum(def, x) => Ok((def, x)),
        x => Err(SchemaExpectedEnum(x)),
    }
}

pub fn take_struct(t: Column) -> Result<(Default, Vec<Field<Column>>), SchemaError> {
    match t {
        Table::Struct(def, x) => Ok((def, x)),
        x => Err(SchemaExpectedStruct(x)),
    }
}

pub fn take_nested(t: Column) -> Result<Table, SchemaError> {
    match t {
        Table::Nested(x) => Ok(x),
        x => Err(SchemaExpectedNested(x)),
    }
}

pub fn take_reversed(t: Column) -> Result<Column, SchemaError> {
    match t {
        Table::Reversed(x) => Ok(x),
        x => Err(SchemaExpectedReversed(x)),
    }
}

pub fn take_option(x0: Column) -> Result<(Default, Column), SchemaError> {
    let (def, vs) = take_enum(x0);
    match &vs[..] {
        [Variant("none", Unit), Variant("some", x)] => Ok((def, x)),
        _ => Err(SchemaExpectedOption(vs)),
    }
}

pub fn take_either(x0: Column) -> Result<(Default, Column, Column), SchemaError> {
    let (def, vs) = take_enum(x0);
    match &vs[..] {
        [Variant("left", l), Variant("right", r)] => Ok((def, l, r)),
        _ => Err(SchemaExpectedOption(vs)),
    }
}

pub fn take_pair(x0: Column) -> Result<(Default, Column, Column), SchemaError> {
    let (def, fs) = take_struct(x0);
    match &fs[..] {
        [Field("first", x), Field("second", y)] => Ok((def, x, y)),
        _ => Err(SchemaExpectedPair(fs)),
    }
}

pub fn take_default(t: Table) -> Default {
    match t {
        Table::Binary(def, _) => def,
        Table::Array(def, _) => def,
        Table::Map(def, _) => def,
    }
}

pub fn take_default_column(c: Column) -> Default {
    match c {
        Column::Unit => Default::AllowDefault,
        Column::Int(def, _) => def,
        Column::Double(def, _) => def,
        Column::Enum(def, _) => def,
        Column::Struct(def, _) => def,
        Column::Nested(x) => take_default(x),
        Column::Reversed(x) => take_default_column(x),
    }
}

pub fn with_defualt(def: Default, t: Table) -> Table {
    match t {
        Table::Binary(_, encoding) -> Table::Binary(def, encoding),
        Table::Array(_, x) -> Table::Array(def, x),
        Table::Map(_, k, v) -> Table::Map(def, k, v),
    }
}

pub fn with_default_column(def: Default, c: Column) -> Column {
    match c {
        Column::Unit => Column::Unit ,
        Column::Int(_, encoding) => Column::Int(def, encoding),
        Column::Double(_) => Column::Double(def),
        Column::Enum(_, vs) => Column::Enum(def, vs),
        Column::Struct(_, fs) => Column::Enum(def, fs),
        Column::Nested(x) => Column::Nested(with_default(def, x)),
        Column::Reversed(x) => Column::Reversed(with_default_column(def, x)),
    }
}

pub fn union(t0: Table, t1: Table) -> Result<Table, SchemaUnionError> {
    match (t0, t1) {
        (Table::Binary(def0, encoding0), Table::Binary(def1, encoding1)) if def0 == def1 =>
            Ok(Table::Binary(def0, encoding0)),
        (Table::Array(def0, x0), Table::Array(def1, x1)) if def0 == def1 =>
            Ok(Table::Array(def0, union_column(x0, x1))),
        (Table::Map(def0, k0, v0), Table::Map(def1, k1, v1)) if def0 == def1 =>
            Ok(Table::Map(def0, k0, union_column(v0, v1))),
        (Table::Map(def0, k0, v0), Table::Map(def1, k1, v1)) if k0 != k1 =>
            Err(SchemaUnionMapKeyNotAllowed(t0, t1)),
        _ =>
            Err(SchemaUnionTableMismatch(t0, t1)),
    }
}

pub fn union_column(c0: Column, c1: Column) -> Result<Column, SchemaUnionError> {
    match (c0, c1) {
        (Column::Unit, Column::Unit) =>
            Ok(Column::Unit),
        (Column::Int(def0, encoding0), Column::Int(def1, encoding1)) if def0 == def1 && encoding0 == encoding1 =>
                Ok(Column::Int(def0, encoding0)),
        (Column::Double(def0), Column::Double(def1)) if def0 == def1 =>
            Ok(Column::Double(def0)),
        (Column::Enum(def0, vs0), Column::Enum(def1, vs1)) if def0 == def1 && vs0.into_iter().map(|v| v.name).collect() == vs1.into_iter().map(|v| v.name).collect() =>
            Ok(Column::Enum(def0, vs0.into_iter().zip().map(|(Variant(n, x), Variant(_, y))| Variant(n, union_column(x, y))))),
        (Column::Struct(def0, fs0), Column::Struct(def1, fs1)) if def0 == def1 =>
            Ok(Column::Struct(def0, union_struct(fs0, fs1))),
        (Column::Nested(x0), Column::Nested(x1)) =>
            Ok(Column::Nested(union(x0, x1))),
        (Column::Reversed(x0), Column::Reversed(x1)) =>
            Ok(Column::Reversed(union_column(x0, x1))),
        _ =>
            Err(SchemaUnionColumnMismatch(c0, c1)),
    }
}

pub enum In<A> {
    One(A),
    Both(A, A),
}

pub fn default_or_union(fields: HashMap<FieldName, In<Column>>, field: Column) -> Result<Field<Column>, SchemaUnionError> {
    let Field(name, _) = field.clone();
    match fields.get(name) {
        None => Err(SchemaUnionFailedLookupInternalError(field)),
        Some(In::One(schema)) => match take_default_column(schema) {
            Default::DenyDefualt =>
                Err(SchemaUnionDefaultNotAllowed(field)),
            Default::AllowDefault =>
                Ok(Field(name, schema)),
        }
        Some(In::Both(schema0, schema1)) => {
            Ok(Field(name, union_column(schema0, schema1))),
        },
    }
}

pub fn default_or_nothing(fields: HashMap<FieldName, In<Column>>, field: Column) -> Result<Option<Field<Column>>, SchemaUnionError> {
    let Field(name, _) = field.clone();
    match fields.get(name) {
        None => Err(SchemaUnionFailedLookupInternalError(field)),
        Some(In::One(schema)) => match take_default_column(schema) {
            Default::DenyDefualt =>
                Err(SchemaUnionDefaultNotAllowed(field)),
            Default::AllowDefault =>
                Ok(Some(Field(name, schema))),
        },
        Some(In::Both(_, _)) => {
            Ok(None),
        },
    }
}


pub fn union_struct(
    cfields0: Vec<Field<Column>>,
    cfields1: Vec<Field<Column>>,
) -> Result<Vec<Field<column>>, SchemaUnionError> {
    let fields0: HashSet<FieldName> = cfields0.clone().into_iter().map(|Field(k, _)| k).collect();
    let fields1: HashSet<FieldName> = cfields1.clone().into_iter().map(|Field(k, _)| k).collect();
    // NB. this errs on fetching the One values from the lhs.
    //     but i'm not sure if this is invalid from a correctness pov.
    let sym_diff = fields0.symmetric_difference(&fields1).map(|k| (k, In::One(fields0.get(k))));
    let intersect = fields0.intersection(&fields1).map(|k| (k, In::Both(fields0.get(k), fields1.get(k))));
    let fields = sym_diff.chain(intersect).collect();
    let mut xs: Vec<Option<Field<Column>>> = cfields0.into_iter().map(|x| default_or_union(fields, x)).collect()?;
    let ys: Vec<Option<Field<Column>>> = cfields0.into_iter().map(|x| default_or_nothing(fields, x)).collect()?;
    let ys = ys.into_iter().flat_map(|x| x).collect();
    xs.extend(ys);
    Ok(xs)
}

