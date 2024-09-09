use std::mem::{align_of, size_of};

/// transmute `byte slice` to `T`.
pub fn transmute_from_u8<T>(v: &[u8]) -> &T {
    debug_assert_eq!(v.len(), size_of::<T>());

    debug_assert_eq!(
        v.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        v.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        v.as_ptr().align_offset(align_of::<T>()),
    );

    unsafe {
        let raw_ptr: *const T = v.as_ptr().cast::<T>();
        &*raw_ptr
    }
}

/// transmute `T` to `byte slice`.
pub fn transmute_to_u8<T>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v as *const T as *const u8, std::mem::size_of_val(v)) }
}

/// transmute `T slice` to `byte slice`.
pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// transmute `byte slice` to `T slice`.
pub fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);

    debug_assert_eq!(
        data.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into slice of {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        data.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        data.as_ptr().align_offset(align_of::<T>()),
    );

    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// transmute `mut byte slice` to `mut T slice`.
pub fn transmute_from_u8_to_mut_slice<T>(data: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);

    debug_assert_eq!(
        data.as_ptr().align_offset(align_of::<T>()),
        0,
        "transmuting byte slice {:p} into mutable slice of {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        data.as_ptr(),
        std::any::type_name::<T>(),
        align_of::<T>(),
        data.as_ptr().align_offset(align_of::<T>()),
    );

    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

#[cfg(test)]
mod tests {
    use super::{
        transmute_from_u8, transmute_from_u8_to_mut_slice, transmute_from_u8_to_slice,
        transmute_to_u8, transmute_to_u8_slice,
    };

    #[derive(Debug, PartialEq)]
    struct MyStruct {
        field1: u16,
        field2: u8,
    }

    #[test]
    fn test_transmute_from_u8_to_simple() {
        let bytes: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
        let value: &u32 = transmute_from_u8::<u32>(&bytes);
        assert_eq!(*value, 0x78563412);

        let bytes: [u8; 4] = [0x00, 0x00, 0x80, 0x3f];
        let value: &f32 = transmute_from_u8::<f32>(&bytes);
        assert_eq!(*value, 1.0);
    }

    #[test]
    fn test_transmute_from_u8_to_struct() {
        let need_bytes: usize = std::mem::size_of::<MyStruct>();
        let mut aligned_bytes: Vec<u8> = Vec::<u8>::with_capacity(need_bytes);

        unsafe {
            aligned_bytes.set_len(need_bytes);

            let ptr: *mut u8 = aligned_bytes.as_mut_ptr();

            std::ptr::write(ptr.add(0), 0x34);
            std::ptr::write(ptr.add(1), 0x12);
            std::ptr::write(ptr.add(2), 0x78);
        }

        let value: &MyStruct = transmute_from_u8::<MyStruct>(&aligned_bytes);
        assert_eq!(
            *value,
            MyStruct {
                field1: 0x1234,
                field2: 0x78
            }
        )
    }

    #[test]
    #[should_panic]
    fn test_transmute_from_u8_invalid_length() {
        let bytes = [0x12, 0x34, 0x56];
        transmute_from_u8::<u32>(&bytes);
    }

    #[test]
    fn test_transmute_to_u8() {
        let value: u32 = 0x12345678;
        let bytes: &[u8] = transmute_to_u8(&value);
        assert_eq!(bytes, &[0x78, 0x56, 0x34, 0x12]);

        let value: f32 = 1.0;
        let bytes: &[u8] = transmute_to_u8(&value);
        assert_eq!(bytes, &[0x00, 0x00, 0x80, 0x3f]);

        let value = MyStruct {
            field1: 0x1234,
            field2: 0x56,
        };
        let bytes: &[u8] = transmute_to_u8(&value);
        assert_eq!(bytes, &[0x34, 0x12, 0x56, 0x00]); // 内存对齐
    }

    #[test]
    fn test_transmute_from_u8_to_slice() {
        let bytes: [u8; 8] = [0x12, 0x34, 0x56, 0x78, 0xAB, 0xCD, 0xEF, 0x01];
        let slice: &[u16] = transmute_from_u8_to_slice(&bytes);
        assert_eq!(slice, &[0x3412, 0x7856, 0xCDAB, 0x01EF]);

        let bytes: [u8; 12] = [
            0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40,
        ];
        let slice: &[f32] = transmute_from_u8_to_slice(&bytes);
        assert_eq!(slice, &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_transmute_from_u8_to_mut_slice() {
        let mut bytes: [u8; 8] = [0x12, 0x34, 0x56, 0x78, 0xAB, 0xCD, 0xEF, 0x01];
        let slice: &mut [u16] = transmute_from_u8_to_mut_slice(&mut bytes);
        assert_eq!(slice, &mut [0x3412, 0x7856, 0xCDAB, 0x01EF]);
        slice[0] = 0x0011;
        assert_eq!(bytes, [0x11, 0x00, 0x56, 0x78, 0xAB, 0xCD, 0xEF, 0x01]);
    }

    #[test]
    fn test_transmute_to_u8_slice() {
        let values: [u16; 4] = [0x1234, 0x5678, 0xABCD, 0xEF01];
        let bytes: &[u8] = transmute_to_u8_slice(&values);
        assert_eq!(bytes, &[0x34, 0x12, 0x78, 0x56, 0xCD, 0xAB, 0x01, 0xEF]);

        let values: [f32; 3] = [1.0, 2.0, 3.0];
        let bytes: &[u8] = transmute_to_u8_slice(&values);
        assert_eq!(
            bytes,
            &[0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40,]
        );
    }

    #[test]
    #[should_panic]
    fn test_transmute_from_u8_to_slice_invalid_length() {
        let bytes = [0x12, 0x34, 0x56, 0x78, 0xAB];
        transmute_from_u8_to_slice::<u16>(&bytes);
    }

    #[test]
    #[should_panic]
    fn test_transmute_from_u8_to_mut_slice_invalid_length() {
        let mut bytes = [0x12, 0x34, 0x56, 0x78, 0xAB];
        transmute_from_u8_to_mut_slice::<u16>(&mut bytes);
    }
}
