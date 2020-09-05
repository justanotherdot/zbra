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
