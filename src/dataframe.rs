use rayon::prelude::*;
use failure::Error;

#[derive(Debug)]
pub struct DataFrame {
    // Should this be a HashMap? IndexMap?
    columns: Vec<Column>,
}

impl DataFrame {
    pub fn assign(&mut self, new_col: Column)
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
    UInt8(ArrayData<u8>),
}

impl Array {
    pub fn dtype(&self) -> String {
        match *self {
            Array::Int8(_) => "Int8".to_owned(),
            Array::UInt8(_) => "UInt8".to_owned(),
        }
    }
}

// Use macros to handle all the different types
// basic type and Array type
impl DataType<i8> for Array {
    fn apply_inplace<F>(&mut self, f: F) -> Result<(), Error>
        where F: Fn(&mut i8) + Sync + Send,
    {
        match *self {
            Array::Int8(ref mut array_data) => array_data.apply_inplace(f),
            _ => return Err(format_err!("Fn type is i8 but array is {}", self.dtype())),
        }
        Ok(())
    }

    fn apply<F>(&self, f: F) -> Result<Array, Error>
        where F: Fn(&i8) -> i8 + Sync + Send,
    {
        match *self {
            Array::Int8(ref array_data) => Ok(Array::Int8(array_data.apply(f))),
            _ => Err(format_err!("Fn type is i8 but array is {}", self.dtype())),
        }
    }
}

impl DataType<u8> for Array {
    fn apply_inplace<F>(&mut self, f: F) -> Result<(), Error>
        where F: Fn(&mut u8) + Sync + Send,
    {
        match *self {
            Array::UInt8(ref mut array_data) => array_data.apply_inplace(f),
            _ => return Err(format_err!("Fn type is u8 but array is {}", self.dtype())),
        }
        Ok(())
    }

    fn apply<F>(&self, f: F) -> Result<Array, Error>
        where F: Fn(&u8) -> u8 + Sync + Send,
    {
        match *self {
            Array::UInt8(ref array_data) => Ok(Array::UInt8(array_data.apply(f))),
            _ => Err(format_err!("Fn type is u8 but array is {}", self.dtype())),
        }
    }
}

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
    fn test_dataframe_assign() {
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
        df.assign(new_col);
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
        let array2 = array.apply(test_fn_bad1).unwrap();
 
    }
    #[test]
    #[should_panic]
    fn test_array_apply_wrong_type_u8() {
        let array = Array::UInt8(ArrayData(vec![1,2,3]));
        // Test to see if this compiles
        fn test_fn_bad2(x: &i8) -> i8 {
            x.pow(2)
        }
        let array2 = array.apply(test_fn_bad2).unwrap();
    }

}
