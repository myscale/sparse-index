#[cfg(test)]
mod test{
    use cxx::let_cxx_string;
    use tempfile::TempDir;

    use crate::{ffi::TupleElement, ffi_commit_index, ffi_create_index, ffi_insert_sparse_vector, ffi_load_index_reader, ffi_sparse_search};

    fn mock_row_content(base: u32, rows: u32) -> impl Iterator<Item = Vec<TupleElement>> {
        (base*rows..base*rows+rows).map(|i| {
            (0..768).map(|j| {
                let dim_id = (i + j) % 1000000;
                let weight_f32 = 0.1 + (i + j) as f32;
                
                TupleElement {
                    dim_id,
                    weight_f32,
                    weight_u8: 0,
                    weight_u32: 0,
                    value_type: 0,
                }
            }).collect()
        })
    }

    #[test]
    pub fn test_index_rows() {
        let temp_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();

        // let_cxx_string!(index_path = temp_dir);
        let_cxx_string!(index_path = "/home/mochix/test/sparse_index_files/temp2");
        let res = ffi_create_index(&index_path);
        println!("create - {:?}", res);


        for (row_id, sv) in mock_row_content(0, 10000).enumerate() {
            ffi_insert_sparse_vector(&index_path, row_id as u32, &sv);
        }
        let res = ffi_commit_index(&index_path);
        println!("commit - {:?}", res);

        let res = ffi_load_index_reader(&index_path);
        println!("load - {:?}", res);

        for sv in mock_row_content(10, 100) {
            let res = ffi_sparse_search(&index_path, &sv, &vec![], 10);
            println!("{:?}", res);
        }
    }
}