use std::borrow::Cow;

use crate::schema::{AnyView, View};

pub trait Collection {
    fn name(&self) -> Cow<'static, str>;
    fn add_views(&self, views: &mut CollectionViews<Self>)
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
pub struct CollectionViews<C> {
    views: Vec<Box<dyn AnyView<C>>>,
}

impl<C> CollectionViews<C> {
    pub fn push<V: View<C> + 'static>(&mut self, view: V) {
        self.views.push(view.boxed());
    }
}
