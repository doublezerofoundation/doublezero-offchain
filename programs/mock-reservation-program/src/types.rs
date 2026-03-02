use bytemuck::{Pod, Zeroable};

pub type HistoryIndex = u8;

/// An epoch-stamped entry wrapping arbitrary data `T`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Zeroable)]
#[repr(C, align(8))]
pub struct EpochEntry<T> {
    pub epoch: u64,
    pub data: T,
}

// SAFETY: EpochEntry<T> is #[repr(C, align(8))] with no padding:
// - epoch: u64 at offset 0 (8 bytes, align 8)
// - data: T at offset 8 (T is align(8), so no gap; T: Pod guarantees valid for
//   any bit pattern).
// All fields are Pod, so the whole struct is Pod.
unsafe impl<T: Pod + Default + PartialEq> Pod for EpochEntry<T> {}

impl<T: Pod + Default + PartialEq> Default for EpochEntry<T> {
    fn default() -> Self {
        Zeroable::zeroed()
    }
}

/// A fixed-capacity ring buffer of epoch-stamped entries.
///
/// `N` must fit in a `u8` — the const assert below enforces this.
// TODO: Use u8 instead of usize?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Zeroable)]
#[repr(C, align(8))]
pub struct RingBuffer<T, const N: usize> {
    pub current_index: u8,
    pub total_count: u8,
    _padding: [u8; 6],

    pub entries: [EpochEntry<T>; N],
}

// SAFETY: RingBuffer<T, N> is #[repr(C, align(8))] with no padding.
// All bytes are covered by fields; T: Pod guarantees every bit pattern is
// valid, so the whole struct is Pod.
unsafe impl<T: Pod + Default + PartialEq, const N: usize> Pod for RingBuffer<T, N> {}

impl<T: Pod + Default + PartialEq, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        Zeroable::zeroed()
    }
}

impl<T: Pod + Default + PartialEq, const N: usize> RingBuffer<T, N> {
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Advances the ring to the slot for `epoch` and returns a mutable
    /// reference to it, or returns `None` if the epoch is not monotonically
    /// increasing.
    ///
    /// Specifically:
    /// - If the current entry's epoch already matches `epoch`, returns it
    ///   in-place (idempotent update — no monotonicity violation).
    /// - If `epoch` is strictly greater than the current entry's epoch (or the
    ///   buffer is empty), advances to the next slot, **resets `data` to its
    ///   default value**, stamps the slot with `epoch`, increments
    ///   `total_count` (saturating at `N`), and returns the new slot.
    /// - If `epoch` is less than or equal to the current entry's epoch and
    ///   does not match it, returns `None`.
    ///
    /// The reset on advance is intentional: it prevents stale data from a
    /// previous epoch from being silently carried forward when only some fields
    /// of `T` are written by the caller.
    pub fn advance_mut(&mut self, epoch: u64) -> Option<&mut EpochEntry<T>> {
        if self.total_count > 0 {
            let current_epoch = self.entries[self.current_index as usize].epoch;
            if epoch == current_epoch {
                // Same epoch — return current slot in-place.
                return Some(&mut self.entries[self.current_index as usize]);
            } else if epoch < current_epoch {
                // Epoch is not monotonically increasing.
                return None;
            }
        }

        let write_index = if self.total_count == 0 {
            0
        } else {
            (self.current_index + 1) % N as u8
        };

        self.entries[write_index as usize] = EpochEntry {
            epoch,
            data: Default::default(),
        };
        self.current_index = write_index;
        self.total_count = self.total_count.saturating_add(1).min(N as u8);

        Some(&mut self.entries[self.current_index as usize])
    }

    /// Returns the entry whose epoch matches `epoch`, searching backwards from
    /// the current slot across all active entries. Returns `None` if no match
    /// is found.
    pub fn find(&self, epoch: u64) -> Option<u8> {
        for i in 0..self.total_count as usize {
            let index = (self.current_index as usize + N - i) % N;
            if self.entries[index].epoch == epoch {
                return Some(index as u8);
            }
        }
        None
    }

    /// Returns a reference to the entry at ring slot `index`, or `None` if
    /// `index` is out of bounds.
    pub fn get(&self, index: u8) -> Option<&EpochEntry<T>> {
        if (index as usize) < N {
            Some(&self.entries[index as usize])
        } else {
            None
        }
    }

    /// Returns a mutable reference to the entry at ring slot `index`, or
    /// `None` if `index` is out of bounds.
    pub fn get_mut(&mut self, index: u8) -> Option<&mut EpochEntry<T>> {
        if (index as usize) < N {
            Some(&mut self.entries[index as usize])
        } else {
            None
        }
    }

    /// Returns a reference to an entry by recency offset. `offset=0` is the
    /// most recent entry, `offset=1` is one epoch prior, and so on. Returns
    /// `None` if `offset` is out of range or the buffer is empty.
    pub fn relative_entry(&self, offset: usize) -> Option<&EpochEntry<T>> {
        if offset < self.total_count as usize {
            let index = (self.current_index as usize + N - offset) % N;
            Some(&self.entries[index])
        } else {
            None
        }
    }

    /// Returns the most recently written entry, or `None` if the buffer has
    /// never been written.
    pub fn current_entry(&self) -> Option<&EpochEntry<T>> {
        if self.total_count == 0 {
            None
        } else {
            Some(&self.entries[self.current_index as usize])
        }
    }

    /// Returns `true` once `total_count` has reached `N`.
    pub fn is_at_capacity(&self) -> bool {
        self.total_count >= N as u8
    }
}

//

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
    #[repr(C, align(8))]
    struct TestData {
        value: u64,
    }

    type TestBuf = RingBuffer<TestData, 4>;

    // --- advance_mut ---

    #[test]
    fn empty_buffer_has_no_current_entry() {
        let buf = TestBuf::default();
        assert!(buf.current_entry().is_none());
    }

    #[test]
    fn first_write_uses_index_zero() {
        const EPOCH: u64 = 1;
        const VALUE: u64 = 42;

        let mut buf = TestBuf::default();
        let entry = buf.advance_mut(EPOCH).unwrap();
        entry.data.value = VALUE;

        let entry = *entry;
        assert_eq!(
            entry,
            EpochEntry {
                epoch: EPOCH,
                data: TestData { value: VALUE }
            }
        );

        assert_eq!(buf.current_index, 0);
        assert_eq!(buf.total_count, 1);
    }

    #[test]
    fn same_epoch_updates_in_place() {
        let mut buf = TestBuf::default();
        buf.advance_mut(1).unwrap().data.value = 10;
        buf.advance_mut(1).unwrap().data.value = 20;
        assert_eq!(buf.current_index, 0);
        assert_eq!(buf.total_count, 1);
        assert_eq!(buf.current_entry().unwrap().data.value, 20);
    }

    #[test]
    fn new_epoch_advances_index() {
        let mut buf = TestBuf::default();
        buf.advance_mut(1).unwrap().data.value = 10;
        buf.advance_mut(2).unwrap().data.value = 20;
        assert_eq!(buf.current_index, 1);
        assert_eq!(buf.total_count, 2);
        assert_eq!(buf.current_entry().unwrap().data.value, 20);
    }

    #[test]
    fn advance_resets_data_on_new_epoch() {
        let mut buf = TestBuf::default();
        buf.advance_mut(1).unwrap().data.value = 42;
        let entry = buf.advance_mut(2).unwrap();
        assert_eq!(entry.data.value, 0);
    }

    #[test]
    fn non_monotonic_epoch_returns_none() {
        let mut buf = TestBuf::default();
        buf.advance_mut(5).unwrap();
        assert!(buf.advance_mut(4).is_none());
        assert!(buf.advance_mut(5).is_some()); // equal is still ok (idempotent)
    }

    #[test]
    fn total_count_saturates_at_capacity() {
        let mut buf = TestBuf::default();
        for epoch in 1..=10 {
            buf.advance_mut(epoch);
        }
        assert!(buf.is_at_capacity());
        assert_eq!(
            buf,
            TestBuf {
                current_index: 1,
                total_count: 4,
                _padding: [0; 6],
                entries: [
                    EpochEntry {
                        epoch: 9,
                        data: TestData { value: 0 }
                    },
                    EpochEntry {
                        epoch: 10,
                        data: TestData { value: 0 }
                    },
                    EpochEntry {
                        epoch: 7,
                        data: TestData { value: 0 }
                    },
                    EpochEntry {
                        epoch: 8,
                        data: TestData { value: 0 }
                    },
                ],
            }
        );
    }

    #[test]
    fn ring_wraps_after_capacity() {
        let mut buf = TestBuf::default();
        for epoch in 1..=4 {
            buf.advance_mut(epoch).unwrap().data.value = epoch;
        }
        assert!(buf.is_at_capacity());
        buf.advance_mut(5).unwrap().data.value = 99;
        assert_eq!(buf.current_index, 0);
        assert_eq!(
            buf.entries[0],
            EpochEntry {
                epoch: 5,
                data: TestData { value: 99 }
            }
        );
    }

    #[test]
    fn is_at_capacity_false_then_true() {
        let mut buf = TestBuf::default();
        for epoch in 1..=3 {
            buf.advance_mut(epoch);
            assert!(!buf.is_at_capacity());
        }
        buf.advance_mut(4);
        assert!(buf.is_at_capacity());
    }

    #[test]
    fn current_entry_reflects_latest_write() {
        let mut buf = TestBuf::default();
        for epoch in 1..=4 {
            buf.advance_mut(epoch).unwrap().data.value = epoch * 10;
            assert_eq!(buf.current_entry().unwrap().data.value, epoch * 10);
        }
    }

    // --- find ---

    #[test]
    fn find_returns_none_on_empty_buffer() {
        let buf = TestBuf::default();
        assert!(buf.find(1).is_none());
    }

    #[test]
    fn find_locates_current_epoch() {
        let mut buf = TestBuf::default();
        buf.advance_mut(7).unwrap();
        assert_eq!(buf.find(7), Some(0));
    }

    #[test]
    fn find_locates_older_epoch() {
        let mut buf = TestBuf::default();
        for epoch in 1..=3 {
            buf.advance_mut(epoch).unwrap();
        }
        assert_eq!(buf.find(1), Some(0));
        assert_eq!(buf.find(2), Some(1));
        assert_eq!(buf.find(3), Some(2));
    }

    #[test]
    fn find_returns_none_for_evicted_epoch() {
        let mut buf = TestBuf::default();
        for epoch in 1..=5 {
            buf.advance_mut(epoch).unwrap();
        }
        // epoch 1 was overwritten when the ring wrapped
        assert!(buf.find(1).is_none());
    }

    // --- get / get_mut ---

    #[test]
    fn get_returns_entry_for_valid_index() {
        let mut buf = TestBuf::default();
        buf.advance_mut(10).unwrap().data.value = 55;
        assert_eq!(buf.get(0).unwrap().data.value, 55);
    }

    #[test]
    fn get_returns_none_for_out_of_bounds_index() {
        let buf = TestBuf::default();
        assert!(buf.get(4).is_none());
    }

    #[test]
    fn get_mut_allows_direct_modification() {
        let mut buf = TestBuf::default();
        buf.advance_mut(10).unwrap();
        buf.get_mut(0).unwrap().data.value = 77;
        assert_eq!(buf.current_entry().unwrap().data.value, 77);
    }

    #[test]
    fn get_mut_returns_none_for_out_of_bounds_index() {
        let mut buf = TestBuf::default();
        assert!(buf.get_mut(4).is_none());
    }

    // --- relative_entry ---

    #[test]
    fn relative_entry_returns_none_on_empty_buffer() {
        let buf = TestBuf::default();
        assert!(buf.relative_entry(0).is_none());
    }

    #[test]
    fn relative_entry_zero_matches_current_entry() {
        let mut buf = TestBuf::default();
        buf.advance_mut(1).unwrap().data.value = 10;
        buf.advance_mut(2).unwrap().data.value = 20;
        assert_eq!(buf.relative_entry(0), buf.current_entry());
    }

    #[test]
    fn relative_entry_walks_back_in_time() {
        let mut buf = TestBuf::default();
        for epoch in 1..=4 {
            buf.advance_mut(epoch).unwrap().data.value = epoch * 10;
        }
        assert_eq!(buf.relative_entry(0).unwrap().data.value, 40);
        assert_eq!(buf.relative_entry(1).unwrap().data.value, 30);
        assert_eq!(buf.relative_entry(2).unwrap().data.value, 20);
        assert_eq!(buf.relative_entry(3).unwrap().data.value, 10);
    }

    #[test]
    fn relative_entry_returns_none_beyond_total_count() {
        let mut buf = TestBuf::default();
        buf.advance_mut(1).unwrap();
        buf.advance_mut(2).unwrap();
        assert!(buf.relative_entry(2).is_none());
    }
}
