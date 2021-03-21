use sha2::{Digest, Sha256};

/// a struct containing information about a `Document`'s revision history
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Revision {
    /// The current revision id of the document. This value is sequentially incremented on each document update.
    pub id: usize,
    /// The SHA256 digest of the bytes contained within the `Document`.
    pub sha256: [u8; 32],
}

impl Revision {
    /// creates the first revision for a document with the SHA256 digest of the passed bytes
    #[must_use]
    pub fn new(contents: &[u8]) -> Self {
        Self {
            id: 0,
            sha256: digest(contents),
        }
    }

    /// creates the next revision in sequence with an updated digest. If the digest doesn't change, None is returned.
    ///
    /// # Panics
    ///
    /// Panics if `id` overflows
    #[must_use]
    pub fn next_revision(&self, new_contents: &[u8]) -> Option<Self> {
        let sha256 = digest(new_contents);
        if sha256 == self.sha256 {
            None
        } else {
            Some(Self {
                id: self
                    .id
                    .checked_add(1)
                    .expect("need to implement revision id wrapping or increase revision id size"),
                sha256,
            })
        }
    }
}

fn digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::default();
    hasher.update(payload);
    hasher.finalize().into()
}

#[test]
fn revision_tests() {
    let original_contents = b"one";
    let first_revision = Revision::new(original_contents);
    let original_digest =
        hex_literal::hex!("7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed");
    assert_eq!(
        first_revision,
        Revision {
            id: 0,
            sha256: original_digest
        }
    );
    assert!(first_revision.next_revision(original_contents).is_none());

    let updated_contents = b"two";
    let next_revision = first_revision
        .next_revision(updated_contents)
        .expect("new contents should create a new revision");
    assert_eq!(
        next_revision,
        Revision {
            id: 1,
            sha256: hex_literal::hex!(
                "3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3"
            )
        }
    );
    assert!(next_revision.next_revision(updated_contents).is_none());

    assert_eq!(
        next_revision.next_revision(original_contents),
        Some(Revision {
            id: 2,
            sha256: original_digest
        })
    )
}
