/// Configuration options for [`Storage`].
#[derive(Default, Debug)]
pub struct Configuration {
    /// Configuration options related to background tasks.
    pub workers: Tasks,

    /// Configuration options related to views.
    pub views: Views,
}

/// Configujration options for background tasks.
#[derive(Debug)]
pub struct Tasks {
    /// Defines how many workers should be spawned to process tasks. Default
    /// value is `16`.
    pub worker_count: usize,
}

impl Default for Tasks {
    fn default() -> Self {
        Self {
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            worker_count: 16,
        }
    }
}

/// Configuration options for views.
#[derive(Debug)]
pub struct Views {
    /// If true, the database will scan all views during the call to
    /// `open_local`. This will cause database opening to take longer, but once
    /// the database is open, no request will need to wait for the integrity to
    /// be checked. However, for faster startup time, you may wish to delay the
    /// integrity scan. Default value is `false`.
    pub check_integrity_on_open: bool,
}

impl Default for Views {
    fn default() -> Self {
        Self {
            check_integrity_on_open: false,
        }
    }
}
