use crate::RowId;

#[derive(Debug, Clone)]
pub struct SparseBitmap {
    alive_row_ids: Vec<u8>,
}

impl Default for SparseBitmap {
    fn default() -> Self {
        Self { alive_row_ids: vec![] }
    }
}

impl SparseBitmap {
    pub fn is_alive(&self, row_id: u32) -> bool {
        let idx = row_id / 8;
        if idx >= self.alive_row_ids.len() as u32 {
            return false;
        }
        let offset = row_id % 8;
        let byte = self.alive_row_ids[idx as usize];
        (byte & (1 << offset)) != 0
    }
}

impl From<Vec<RowId>> for SparseBitmap {
    fn from(value: Vec<RowId>) -> Self {
        // O(n) try get max row_id, we use it to calculate bitmap(u8 vec) size
        let max_row_id = match value.iter().max() {
            Some(&max) => max,
            None => return Self { alive_row_ids: vec![] },
        };
        let u8_bitmap_size = (max_row_id as usize / 8) + 1;
        let mut bitmap = vec![0u8; u8_bitmap_size];

        for &row_id in &value {
            let byte_index = (row_id / 8) as usize;
            let bit_index = row_id % 8;
            bitmap[byte_index] |= 1 << bit_index;
        }

        Self { alive_row_ids: bitmap }
    }
}

impl From<Vec<u8>> for SparseBitmap {
    fn from(value: Vec<u8>) -> Self {
        Self { alive_row_ids: value }
    }
}

impl Into<Vec<RowId>> for SparseBitmap {
    fn into(self) -> Vec<RowId> {
        let mut row_ids = Vec::new();
        for (i, &byte) in self.alive_row_ids.iter().enumerate() {
            for j in 0..8 {
                if byte & (1 << j) != 0 {
                    row_ids.push((i * 8 + j) as u32);
                }
            }
        }
        row_ids
    }
}
