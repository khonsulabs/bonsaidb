use std::borrow::Cow;

use crate::schema::{view, View};

pub trait Collection: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn define_views(&self, views: &mut Views<Self>)
    where
        Self: Sized;

    fn boxed(self) -> Box<dyn Collection>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[derive(Default)]
pub struct Views<C> {
    views: Vec<Box<dyn view::Serialized<C>>>,
}

impl<C> Views<C> {
    pub fn push<V: View<C> + 'static>(&mut self, view: V) {
        self.views.push(view.boxed());
    }
}
