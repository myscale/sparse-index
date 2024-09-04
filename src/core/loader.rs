use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead as _, BufReader, Lines};
use std::mem::size_of;
use std::path::Path;

use crate::core::common::mmap_ops::{open_read_mmap, transmute_from_u8, transmute_from_u8_to_slice};
use crate::core::sparse_vector::SparseVector;
use memmap2::Mmap;
use validator::ValidationErrors;

/// Compressed Sparse Row matrix, baked by memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// | name    | type          | size       | start               |
/// |---------|---------------|------------|---------------------|
/// | nrow    | `u64`         | 8          | 0                   |
/// | ncol    | `u64`         | 8          | 8                   |
/// | nnz     | `u64`         | 8          | 16                  |
/// | indptr  | `u64[nrow+1]` | 8*(nrow+1) | 24                  |
/// | indices | `u32[nnz]`    | 4*nnz      | 24+8*(nrow+1)       |
/// | data    | `u32[nnz]`    | 4*nnz      | 24+8*(nrow+1)+4*nnz |
pub struct Csr {
    mmap: Mmap,
    // 矩阵的行数
    nrow: usize,
    // 非零元素的数量
    nnz: usize,
    // 行偏移量数组
    intptr: Vec<u64>,
}

const CSR_HEADER_SIZE: usize = size_of::<u64>() * 3;

impl Csr {
    // 从给定的文件路径打开一个 Csr 对象
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_mmap(open_read_mmap(path.as_ref())?)
    }

    // 返回矩阵的行数
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.nrow
    }

    // 返回一个 CsrIter 迭代器, 用来遍历矩阵的行
    pub fn iter(&self) -> CsrIter<'_> {
        CsrIter { csr: self, row: 0 }
    }

    // 从内存映射文件创建一个 Csr 对象
    fn from_mmap(mmap: Mmap) -> io::Result<Self> {
        // 使用 transmute_from_u8 函数从内存映射对象的前 CSR_HEADER_SIZE 个字节中读取 (u64, u64, u64) 三个值
        // 分别表示矩阵的行数、列数和非零元素的数量。
        let (nrow, ncol, nnz) =
            transmute_from_u8::<(u64, u64, u64)>(&mmap.as_ref()[..CSR_HEADER_SIZE]); // mmap.as_ref() 返回一个字节切片，该切片的前 CSR_HEADER_SIZE 个字节被用于转换。

        // 解引用并更新对应值
        let (nrow, _ncol, nnz) = (*nrow as usize, *ncol as usize, *nnz as usize);

        // 解析 indptr
        let indptr = Vec::from(transmute_from_u8_to_slice::<u64>(
            &mmap.as_ref()[CSR_HEADER_SIZE..CSR_HEADER_SIZE + size_of::<u64>() * (nrow + 1)],
        ));
        // 检查 nptr 是否合法
        if !indptr.windows(2).all(|w| w[0] <= w[1]) || indptr.last() != Some(&(nnz as u64)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid indptr array",
            ));
        }

        Ok(Self {
            mmap,
            nrow,
            nnz,
            intptr: indptr,
        })
    }

    // 通过索引获得对应的稀疏向量
    #[inline]
    unsafe fn vec(&self, row: usize) -> Result<SparseVector, ValidationErrors> {
        // 根据 intptr 获得 SparseVector 在 CSR 的 indices/value 中的分布位置
        let start = *self.intptr.get_unchecked(row) as usize;
        let end = *self.intptr.get_unchecked(row + 1) as usize;

        let mut pos = CSR_HEADER_SIZE + size_of::<u64>() * (self.nrow + 1);

        let indices = transmute_from_u8_to_slice::<u32>(
            self.mmap
                .as_ref()
                .get_unchecked(pos + size_of::<u32>() * start..pos + size_of::<u32>() * end),
        );
        // 向后移动，移动的字节是非零元素数量 * u32
        pos += size_of::<u32>() * self.nnz;

        let data = transmute_from_u8_to_slice::<f32>(
            self.mmap
                .as_ref()
                .get_unchecked(pos + size_of::<f32>() * start..pos + size_of::<f32>() * end),
        );

        SparseVector::new(indices.to_vec(), data.to_vec())
    }
}

/// Iterator over the rows of a CSR matrix.
/// Csr 矩阵行的迭代器, 实现了 Iterator 和 ExactSizeIterator Trait，用于遍历 Csr 矩阵的行
pub struct CsrIter<'a> {
    // 对 Csr 对象的引用
    csr: &'a Csr,
    // 当前行的索引
    row: usize,
}

impl<'a> Iterator for CsrIter<'a> {
    type Item = Result<SparseVector, ValidationErrors>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.row < self.csr.nrow).then(|| {
            let vec = unsafe { self.csr.vec(self.row) };
            self.row += 1;
            vec
        })
    }
}

impl<'a> ExactSizeIterator for CsrIter<'a> {
    fn len(&self) -> usize {
        self.csr.nrow - self.row
    }
}

// 从文件路径加载 Csr 矩阵，并返回一个包含所有稀疏向量的 Vec
pub fn load_csr_vecs(path: impl AsRef<Path>) -> io::Result<Vec<SparseVector>> {
    Csr::open(path)?
        .iter()
        .collect::<Result<Vec<_>, _>>() // collect 跟着的类型参数是什么语法
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// Stream of sparse vectors in JSON format.
// pub struct JsonReader(Lines<BufReader<File>>);
//
// impl JsonReader {
//     pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
//         Ok(JsonReader(BufReader::new(File::open(path)?).lines()))
//     }
// }
//
// impl Iterator for JsonReader {
//     type Item = Result<SparseVector, io::Error>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self.0.next().map(|line| {
//             line.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
//                 .and_then(|line| {
//                     let data: HashMap<String, f32> = serde_json::from_str(&line)?;
//                     SparseVector::new(
//                         data.keys()
//                             .map(|k| k.parse())
//                             .collect::<Result<Vec<_>, _>>()
//                             .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
//                         data.values().copied().collect(),
//                     )
//                         .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
//                 })
//         })
//     }
// }
