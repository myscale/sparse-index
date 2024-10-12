#[macro_export]
macro_rules! row_content(
    () => {
        {
            ($crate::SparseRowContent::default())
        }
    }; // avoids a warning due to the useless `mut`.
    ($($field:expr => $value:expr),*) => {
        {
            let mut row = $crate::core::SparseRowContent::default();
            $(
                row.add_sparse_vector($field, $value);
            )*
            row
        }
    };
    // if there is a trailing comma retry with the trailing comma stripped.
    ($($field:expr => $value:expr),+ ,) => {
        row_content!( $( $field => $value ), *)
    };
);
