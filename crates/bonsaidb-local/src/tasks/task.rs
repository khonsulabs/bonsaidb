use std::borrow::Cow;
use std::sync::Arc;

use crate::tasks::compactor::Compaction;
use crate::views::integrity_scanner::IntegrityScan;
use crate::views::mapper::Map;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Task {
    IntegrityScan(IntegrityScan),
    ViewMap(Map),
    Compaction(Compaction),
    ExpirationLoader(Arc<Cow<'static, str>>),
}
