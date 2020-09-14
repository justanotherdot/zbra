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
    }
}
