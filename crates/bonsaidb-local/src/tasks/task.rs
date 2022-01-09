use std::{borrow::Cow, sync::Arc};

use crate::{
    tasks::compactor::Compaction,
    views::{integrity_scanner::IntegrityScan, mapper::Map},
};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Task {
    IntegrityScan(IntegrityScan),
    ViewMap(Map),
    Compaction(Compaction),
    ExpirationLoader(Arc<Cow<'static, str>>),
}
