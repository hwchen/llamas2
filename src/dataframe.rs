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

//    pub fn melt(self) -> DataFrame {
//    }
//    pub fn get_col(self) -> DataFrame {
//    }
//    pub fn pivot(self) -> DataFrame {
//    }
//    pub fn read_csv(self) -> DataFrame {
//    }
//    implement display
}

#[derive(Debug)]
pub struct Column {
    name: String,
    data: Array,
}

/// This indirection allows for different generic types to
/// be contained in one DataFrame. Alternatively, could
/// be implemented using Traits instead of Enums.
#[derive(Debug)]
pub enum Array {
    Int8(ArrayData<i8>),
    UInt8(ArrayData<u8>),
}

// Use macros to handle all the different types
// basic type and Array type
impl Array {
    pub fn apply_inplace<F>(&mut self, f: F)
        where F: Fn(&mut i8) + Sync + Send,
    {
        match *self {
            Array::Int8(ref mut array_data) => array_data.apply_inplace(f),
            // TODO stuck here... can't dispatch dynamically here,
            _ => (), // doesn't need to return error because simply doesn't compile
        }
    }

    pub fn apply<F>(&self, f: F) -> Array
        where F: Fn(&i8) -> i8 + Sync + Send,
    {
        match *self {
            Array::Int8(ref array_data) => Array::Int8(array_data.apply(f)),
            _ => Array::Int8(ArrayData(vec![])), // doesn't need to return error because simply doesn't compile? TODO
        }
    }
//    pub fn split_str(self) -> DataFrame {
//    }
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
        panic!();
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
            data: df.columns[1].data.apply(|&x| x-2),
        };
        df.assign(new_col);
        println!("{:?}", df);
        panic!();
    }

    #[test]
    fn test_array_apply_inplace() {
        let mut array = Array::Int8(ArrayData(vec![1,2,3]));
        array.apply_inplace(|x| *x = *x*2);
        println!("{:?}", array);
        fn test_fn(x: &mut i8) {
            *x = x.pow(2);
        }
        array.apply_inplace(test_fn);
        println!("{:?}", array);
        panic!();
    }

    #[test]
    fn test_array_apply() {
        let array = Array::Int8(ArrayData(vec![1,2,3]));
        let array1 = array.apply(|&x| x*2);
        println!("{:?}", array1);
        fn test_fn(x: &i8) -> i8 {
            x.pow(2)
        }

        // Test to see if this compiles
        //fn test_fn_bad(x: String) -> String {
        //    x.trim().to_owned()
        //}
        //let array2 = array.apply(test_fn_bad);

        let array2 = array.apply(test_fn);
        println!("{:?}", array2);
        panic!();
    }

    #[test]
    fn test_array_apply_u8() {
        let array = Array::UInt8(ArrayData(vec![1,2,3]));
        let array1 = array.apply(|&x| x*2);
        println!("{:?}", array1);
        fn test_fn(x: &u8) -> u8 {
            x.pow(2)
        }

        // Test to see if this compiles
        //fn test_fn_bad(x: &i8) -> i8 {
        //    x.pow(2)
        //}
        //let array2 = array.apply(test_fn_bad);

        let array2 = array.apply(test_fn);
        println!("{:?}", array2);
        panic!();
    }
}
