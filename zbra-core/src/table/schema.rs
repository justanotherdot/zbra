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
