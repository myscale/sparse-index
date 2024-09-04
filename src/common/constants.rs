use cxx::{CxxString, CxxVector};
use cxx::private::VectorElement;
use once_cell::sync::Lazy;
use crate::common::converter::{Converter, CxxElementStrategy, CxxVectorStrategy, CxxVectorStringStrategy};
use crate::index::{SparseIndexRamBuilderCache};

/// Convert 'CxxString' to 'String'
pub static CXX_STRING_CONVERTER: Lazy<Converter<CxxString, String, CxxElementStrategy>> =
    Lazy::new(|| Converter::new(CxxElementStrategy));

/// Convert 'CxxVector<CxxString>' to 'Vec<String>'
pub static CXX_VECTOR_STRING_CONVERTER: Lazy<
    Converter<CxxVector<CxxString>, Vec<String>, CxxVectorStringStrategy>,
> = Lazy::new(|| Converter::new(CxxVectorStringStrategy));

/// Convert 'CxxVector<T> to Vec<T>'
pub fn cxx_vector_converter<T>() -> Converter<CxxVector<T>, Vec<T>, CxxVectorStrategy<T>>
where
    T: Clone + VectorElement,
{
    Converter::new(CxxVectorStrategy::new())
}

pub static RAM_BUILDER_CACHE: Lazy<SparseIndexRamBuilderCache> =
    Lazy::new(|| SparseIndexRamBuilderCache::new());

// pub static INVERTED_INDEX_CACHE: Lazy<InvertedIndexCache> =
//     Lazy::new(|| InvertedIndexCache::new());


/// copy from cxx
/// `$kind:ident` 类型的种类
/// `$segment:expr` 用于链接名称的标识符
/// `$name:expr` 类型名称, 用于打印或日志
/// `$ty:ty` 将要实现 VecElement 的具体 rust 类型
macro_rules! impl_vector_element {
    ($kind:ident, $segment:expr, $name:expr, $ty:ty) => {
        const_assert_eq!(0, mem::size_of::<CxxVector<$ty>>());
        const_assert_eq!(1, mem::align_of::<CxxVector<$ty>>());

        unsafe impl VectorElement for $ty {
            fn __typename(f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str($name)
            }
            fn __vector_new() -> *mut CxxVector<Self> {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$std$vector$", $segment, "$new")]
                    fn __vector_new() -> *mut CxxVector<$ty>;
                }
                unsafe { __vector_new() }
            }
            fn __vector_size(v: &CxxVector<$ty>) -> usize {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$std$vector$", $segment, "$size")]
                    fn __vector_size(_: &CxxVector<$ty>) -> usize;
                }
                unsafe { __vector_size(v) }
            }
            unsafe fn __get_unchecked(v: *mut CxxVector<$ty>, pos: usize) -> *mut $ty {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$std$vector$", $segment, "$get_unchecked")]
                    fn __get_unchecked(_: *mut CxxVector<$ty>, _: usize) -> *mut $ty;
                }
                unsafe { __get_unchecked(v, pos) }
            }
            vector_element_by_value_methods!($kind, $segment, $ty);
            fn __unique_ptr_null() -> MaybeUninit<*mut c_void> {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$unique_ptr$std$vector$", $segment, "$null")]
                    fn __unique_ptr_null(this: *mut MaybeUninit<*mut c_void>);
                }
                let mut repr = MaybeUninit::uninit();
                unsafe { __unique_ptr_null(&mut repr) }
                repr
            }
            unsafe fn __unique_ptr_raw(raw: *mut CxxVector<Self>) -> MaybeUninit<*mut c_void> {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$unique_ptr$std$vector$", $segment, "$raw")]
                    fn __unique_ptr_raw(this: *mut MaybeUninit<*mut c_void>, raw: *mut CxxVector<$ty>);
                }
                let mut repr = MaybeUninit::uninit();
                unsafe { __unique_ptr_raw(&mut repr, raw) }
                repr
            }
            unsafe fn __unique_ptr_get(repr: MaybeUninit<*mut c_void>) -> *const CxxVector<Self> {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$unique_ptr$std$vector$", $segment, "$get")]
                    fn __unique_ptr_get(this: *const MaybeUninit<*mut c_void>) -> *const CxxVector<$ty>;
                }
                unsafe { __unique_ptr_get(&repr) }
            }
            unsafe fn __unique_ptr_release(mut repr: MaybeUninit<*mut c_void>) -> *mut CxxVector<Self> {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$unique_ptr$std$vector$", $segment, "$release")]
                    fn __unique_ptr_release(this: *mut MaybeUninit<*mut c_void>) -> *mut CxxVector<$ty>;
                }
                unsafe { __unique_ptr_release(&mut repr) }
            }
            unsafe fn __unique_ptr_drop(mut repr: MaybeUninit<*mut c_void>) {
                extern "C" {
                    #[link_name = concat!("cxxbridge1$unique_ptr$std$vector$", $segment, "$drop")]
                    fn __unique_ptr_drop(this: *mut MaybeUninit<*mut c_void>);
                }
                unsafe { __unique_ptr_drop(&mut repr) }
            }
        }
    };
}