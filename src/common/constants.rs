use crate::common::converter::{
    Converter, CxxElementStrategy, CxxVectorStrategy, CxxVectorStringStrategy,
};
use cxx::private::VectorElement;
use cxx::{CxxString, CxxVector};
use once_cell::sync::Lazy;

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


