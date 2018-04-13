use rayon::prelude::*;
use failure::Error;

#[derive(Debug)]
pub struct DataFrame {
    // Should this be a HashMap? IndexMap?
    columns: Vec<Column>,
}

impl DataFrame {
    pub fn add_col(&mut self, new_col: Column)
    {
        self.columns.push(new_col);
    }
}

#[derive(Debug)]
pub struct Column {
    name: String,
    data: Array,
}

pub trait DataType<T> {
    fn apply_inplace<F>(&mut self, f: F) -> Result<(), Error>
        where F: Fn(&mut T) + Sync + Send;

    fn apply<F>(&self, f: F) -> Result<Array, Error>
        where F: Fn(&T) -> T + Sync + Send;
}

/// This indirection allows for different generic types to
/// be contained in one DataFrame. Alternatively, could
/// be implemented using Traits instead of Enums.
#[derive(Debug)]
pub enum Array {
    Int8(ArrayData<i8>),
    Int16(ArrayData<i16>),
    Int32(ArrayData<i32>),
    Int64(ArrayData<i64>),
    UInt8(ArrayData<u8>),
    UInt16(ArrayData<u16>),
    UInt32(ArrayData<u32>),
    UInt64(ArrayData<u64>),
    Float32(ArrayData<f32>),
    Float64(ArrayData<f64>),
    Str(ArrayData<String>),
}

impl Array {
    pub fn dtype(&self) -> String {
        match *self {
            Array::Int8(_) => "Int8".to_owned(),
            Array::Int16(_) => "Int16".to_owned(),
            Array::Int32(_) => "Int32".to_owned(),
            Array::Int64(_) => "Int64".to_owned(),
            Array::UInt8(_) => "UInt8".to_owned(),
            Array::UInt16(_) => "UInt16".to_owned(),
            Array::UInt32(_) => "UInt32".to_owned(),
            Array::UInt64(_) => "UInt64".to_owned(),
            Array::Float32(_) => "Float32".to_owned(),
            Array::Float64(_) => "Float64".to_owned(),
            Array::Str(_) => "Str".to_owned(),
        }
    }
}

// TODO add error type which will give better info
// Also, the error should give a runtime error which points
// at the offending code
macro_rules! impl_datatype_for_array {
    ($t:ty, $p: path) => {
        impl DataType<$t> for Array {
            fn apply_inplace<F>(&mut self, f: F) -> Result<(), Error>
                where F: Fn(&mut $t) + Sync + Send,
            {
                match *self {
                    $p(ref mut array_data) => array_data.apply_inplace(f),
                    _ => return Err(format_err!("Fn type mismatch, array is {}", self.dtype())),
                }
                Ok(())
            }

            fn apply<F>(&self, f: F) -> Result<Array, Error>
                where F: Fn(&$t) -> $t + Sync + Send,
            {
                match *self {
                    $p(ref array_data) => Ok($p(array_data.apply(f))),
                    _ => Err(format_err!("Fn type mismatch, array is {}", self.dtype())),
                }
            }

        }
    };
}

impl_datatype_for_array!(i8, Array::Int8);
impl_datatype_for_array!(i16, Array::Int16);
impl_datatype_for_array!(i32, Array::Int32);
impl_datatype_for_array!(i64, Array::Int64);
impl_datatype_for_array!(u8, Array::UInt8);
impl_datatype_for_array!(u16, Array::UInt16);
impl_datatype_for_array!(u32, Array::UInt32);
impl_datatype_for_array!(u64, Array::UInt64);
impl_datatype_for_array!(f32, Array::Float32);
impl_datatype_for_array!(f64, Array::Float64);
impl_datatype_for_array!(String, Array::Str);

#[derive(Debug)]
pub struct ArrayData<T>(Vec<T>);

impl<T: Send + Sync> ArrayData<T> {
    pub fn apply_inplace<F>(&mut self, f: F)
        where F: Fn(&mut T) + Sync + Send
    {
        self.0.par_iter_mut().for_each(f);
    }

    pub fn apply<F>(&self, f: F) -> Self
        where F: Fn(&T) -> T + Sync + Send
    {
        ArrayData(
            self.0.par_iter()
                .map(f)
                .collect::<Vec<_>>()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dataframe_basic() {
        let df = DataFrame {
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    data: Array::Int8(ArrayData(vec![1,2,3,4,5])),
                },
                Column {
                    name: "population".to_owned(),
                    data: Array::Int8(ArrayData(vec![42,22,63,34,53])),
                }
            ]
        };
        println!("{:?}", df);
    }

    #[test]
    fn test_dataframe_add_col() {
        let mut df = DataFrame {
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    data: Array::Int8(ArrayData(vec![1,2,3,4,5])),
                },
                Column {
                    name: "population".to_owned(),
                    data: Array::Int8(ArrayData(vec![42,22,63,34,53])),
                }
            ]
        };
        println!("{:?}", df);
        let new_col = Column {
            name: "new_col".to_owned(),
            data: df.columns[1].data.apply(|&x: &i8| x-2).unwrap(),
        };
        df.add_col(new_col);
        println!("{:?}", df);
    }

    #[test]
    fn test_array_apply_inplace() {
        let mut array = Array::Int8(ArrayData(vec![1,2,3]));
        array.apply_inplace(|x: &mut i8| *x = *x*2).unwrap();
        println!("{:?}", array);
        fn test_fn(x: &mut i8) {
            *x = x.pow(2);
        }
        array.apply_inplace(test_fn).unwrap();
        println!("{:?}", array);
    }

    #[test]
    fn test_array_apply() {
        let array = Array::Int8(ArrayData(vec![1,2,3]));
        let array1 = array.apply(|&x: &i8| x*2).unwrap();
        println!("{:?}", array1);
        fn test_fn1(x: &i8) -> i8 {
            x.pow(2)
        }

        let array2 = array.apply(test_fn1).unwrap();
        println!("{:?}", array2);

        let array = Array::UInt8(ArrayData(vec![1,2,3]));
        let array1 = array.apply(|&x: &u8| x*2);
        println!("{:?}", array1);
        fn test_fn2(x: &u8) -> u8 {
            x.pow(2)
        }

        let array2 = array.apply(test_fn2).unwrap();
        println!("{:?}", array2);
    }

    // Need to do all the runtime tests for wrong types,
    // since compiler can't pick up
    #[test]
    #[should_panic]
    fn test_array_apply_wrong_type_i8() {
        let array = Array::Int8(ArrayData(vec![1,2,3]));

        // Test to see if this compiles
        fn test_fn_bad1(x: &u8) -> u8 {
            x.pow(2)
        }
        array.apply(test_fn_bad1).unwrap();
 
    }
    #[test]
    #[should_panic]
    fn test_array_apply_wrong_type_u8() {
        let array = Array::UInt8(ArrayData(vec![1,2,3]));
        // Test to see if this compiles
        fn test_fn_bad2(x: &i8) -> i8 {
            x.pow(2)
        }
        array.apply(test_fn_bad2).unwrap();
    }

}
