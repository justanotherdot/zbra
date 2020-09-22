#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Hash)]
pub struct FieldName(String);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Field<A> {
    name: FieldName,
    data: A,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct VariantName(String);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Variant<A>
where
    A: Clone,
{
    name: VariantName,
    data: A,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Tag(i64);

/// RJS: lifted this.
///
/// Ideally this would contain a Zebra.Table.Logical.Table/Value which is the
/// default value for the Table/Column. However, all we need right now is to
/// be able to default to empty lists/maps and 'none' enum values, so we go
/// for a simpler approach where the default value is implied.
pub enum Default {
    /// Table/column can NOT be replaced by a default value if missing.
    DenyDefault,
    /// Table/column can be replaced by a default value if missing.
    AllowDefault,
}

// is `Cons Boxed.Vector (Variant a)` just a way to do NonEmpty?
impl Tag {
    #[inline]
    pub fn has_variant<A: Clone>(&self, xs: Vec<Variant<A>>) -> bool {
        // casting here is iffy.
        (self.0 as usize) < xs.len()
    }

    #[inline]
    pub fn lookup_variant<A: Clone>(&self, xs: Vec<Variant<A>>) -> Option<Variant<A>> {
        // casting here is iffy.
        xs.get(self.0 as usize).cloned()
    }

    // xmap
    // ximap
    // cmap
    // cimap
}

// transmute between Vec<Tag> -> Vec<i64>
// foreign_of_tags
// transmute between Vec<i64> -> Vec<Tag>
// tags_of_foreign
