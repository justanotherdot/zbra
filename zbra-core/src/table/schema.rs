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
