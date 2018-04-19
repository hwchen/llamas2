use failure::Error;
use indexmap::IndexMap;
use rayon::prelude::*;

// TODO remove Array, just use DataFrame, Column, and Array (and in future Buffer
// can replace Vec for lowest level)
// Nulls are handled at the Column level (as high up as possible)

#[derive(Debug, Clone)]
pub struct DataFrame {
    pub columns: IndexMap<String, Array>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            columns: IndexMap::new(),
        }
    }

    pub fn add_col(&mut self, name: String, new_array: Array)
    {
        self.columns.insert(name, new_array);
    }

    pub fn get_col<'a>(&'a self, col_name: & str) -> Option<&'a Array> {
        self.columns.get(col_name)
    }
}

// Why am I using a macro? Because I want the type of the value col
// to be specified, and that can only be done in a macro.
//
// var col is always string typed
//
// the id cols are going to be made from the original cols using a
// pub(crate) method, so that the type will be known.
//
// I think that this covers the type trickery necessary for melt.
#[macro_export]
macro_rules! melt {
    (
    df=$old_df:ident,
    id_vars=[$($id_var:expr),+],
    value_vars=[$(($value_var:expr, $value_var_type:ty)),+],
    value_primitive_type=$value_primitive_type:ty,
    value_type=$value_type:expr,
    var_name=$var_name:expr,
    value_name=$value_name:expr
    ) => {{
        let mut df = DataFrame::new();

        // Assert value_vars exist (id_vars can be asserted as they're
        // actually used, but don't want to waste time with that if value_vars
        // don't exist
        //
        // In conjunction, can count how many times each of the id_vars will
        // be multiplied by.
        let id_vars_row_mult = {
            let mut count = 0;
            $(
                $old_df.get_col($value_var).expect("value_var not found in columns");
                count += 1;
            )+
            count
        };

        // new cols will be in order of id_vars, then var col, then value col
        // also get max len of id_var, which will currently set the overall df len
        // for now. TODO should I use an attribute df.len?
        let mut df_len = 0;
        $(
            // create new col with each row multiplied
            // times id_vars_row_mult, and put in new Dataframe
            // TODO figure out the error handling here. Can't early
            // return from block.
            let old_array = $old_df.get_col($id_var).expect("id_var not found in cols");
            let array_len = old_array.len();
            if array_len > df_len {
                df_len = array_len;
            }

            let new_array = old_array.multiply_row(id_vars_row_mult);

            df.add_col($id_var.to_string(), new_array);
        )+

        // Now the value_vars col names get put into a col
        // Since we previously asserted that they exist, can just make a vec
        // that repeats in the iterator here
        let mut value_vars = vec![];
        $(
            value_vars.push($value_var);
        )+

        let mut var_col = Array::new("Str").expect("couldn't create col");
        // TODO create extend so don't have to use push
        for _ in 0..df_len {
            for v in &value_vars {
                var_col.push(v.to_string());
            }
        }
        df.add_col($var_name.to_string(), var_col);

        // now the values from the value_vars columns
        let mut value_col = Array::new($value_type).expect("couldn't create col");

        for i in 0..df_len {
            $(
                let col = $old_df.get_col($value_var).expect("value_var not found in columns");
                let v: Result<Option<Option<&$value_var_type>>, Error> = col.get(i);
                let v = v
                    .expect("Wrong type")
                    .expect(format!("Could not find index {} in col", i).as_str())
                    .unwrap() // this second unwrap is because no nulls impl yet
                    .clone();
                value_col.push(v as $value_primitive_type);
            )+
        }

        df.add_col($value_name.to_string(), value_col);

        // Done
        df
    }}
}


pub trait DataType<T> {
    fn apply_inplace<F>(&mut self, f: F) -> Result<(), Error>
        where F: Fn(&mut T) + Sync + Send;

    fn apply<F>(&self, f: F) -> Result<Array, Error>
        where F: Fn(&T) -> T + Sync + Send;

    fn get(&self, index: usize) -> Result<Option<Option<&T>>, Error>;

    fn push(&mut self, item: T) -> Result<(), Error>;
}

/// This indirection allows for different generic types to
/// be contained in one DataFrame. Alternatively, could
/// be implemented using Traits instead of Enums.
#[derive(Debug, Clone)]
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
    pub fn new(dtype: &str) -> Result<Self, Error> {
        match dtype {
            "Int8" => Ok(Array::Int8(ArrayData(vec![]))),
            "Int16" => Ok(Array::Int16(ArrayData(vec![]))),
            "Int32" => Ok(Array::Int32(ArrayData(vec![]))),
            "Int64" => Ok(Array::Int64(ArrayData(vec![]))),
            "UInt8" => Ok(Array::UInt8(ArrayData(vec![]))),
            "UInt16" => Ok(Array::UInt16(ArrayData(vec![]))),
            "UInt32" => Ok(Array::UInt32(ArrayData(vec![]))),
            "UInt64" => Ok(Array::UInt64(ArrayData(vec![]))),
            "Float32" => Ok(Array::Float32(ArrayData(vec![]))),
            "Float64" => Ok(Array::Float64(ArrayData(vec![]))),
            "Str" => Ok(Array::Str(ArrayData(vec![]))),
            _ => Err(format_err!("dtype {} not found", dtype)),
        }

    }

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

    pub fn primitive_dtype(&self) -> String {
        match *self {
            Array::Int8(_) => "i8".to_owned(),
            Array::Int16(_) => "i16".to_owned(),
            Array::Int32(_) => "i32".to_owned(),
            Array::Int64(_) => "i64".to_owned(),
            Array::UInt8(_) => "u8".to_owned(),
            Array::UInt16(_) => "u16".to_owned(),
            Array::UInt32(_) => "u32".to_owned(),
            Array::UInt64(_) => "u64".to_owned(),
            Array::Float32(_) => "f32".to_owned(),
            Array::Float64(_) => "f64".to_owned(),
            Array::Str(_) => "String".to_owned(),
        }
    }

    pub fn multiply_row(&self, multiple: usize) -> Self {
        use self::Array::*;
        match *self {
            Int8(ref array_data) => Int8(array_data.multiply_row(multiple)),
            Int16(ref array_data) => Int16(array_data.multiply_row(multiple)),
            Int32(ref array_data) => Int32(array_data.multiply_row(multiple)),
            Int64(ref array_data) => Int64(array_data.multiply_row(multiple)),
            UInt8(ref array_data) => UInt8(array_data.multiply_row(multiple)),
            UInt16(ref array_data) => UInt16(array_data.multiply_row(multiple)),
            UInt32(ref array_data) => UInt32(array_data.multiply_row(multiple)),
            UInt64(ref array_data) => UInt64(array_data.multiply_row(multiple)),
            Float32(ref array_data) => Float32(array_data.multiply_row(multiple)),
            Float64(ref array_data) => Float64(array_data.multiply_row(multiple)),
            Str(ref array_data) => Str(array_data.multiply_row(multiple)),
        }
    }

    pub fn len(&self) -> usize {
        use self::Array::*;
        match *self {
            Int8(ref array_data) => array_data.len(),
            Int16(ref array_data) => array_data.len(),
            Int32(ref array_data) => array_data.len(),
            Int64(ref array_data) => array_data.len(),
            UInt8(ref array_data) => array_data.len(),
            UInt16(ref array_data) => array_data.len(),
            UInt32(ref array_data) => array_data.len(),
            UInt64(ref array_data) => array_data.len(),
            Float32(ref array_data) => array_data.len(),
            Float64(ref array_data) => array_data.len(),
            Str(ref array_data) => array_data.len(),
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

            /// Result is for whether or not there's a runtime eror
            /// First Option is whether a value existed at the requested index
            /// Second Option is whether that value is null
            fn get(&self, index: usize) -> Result<Option<Option<&$t>>, Error> {
                // replace with inner instead of 0
                // TODO there needs to be a match
                match *self {
                    $p(ref array_data) => Ok(array_data.get(index)),
                    _ => Err(format_err!("type mismatch, array is {}", self.dtype())),
                }
            }

            fn push(&mut self, item: $t) -> Result<(), Error> {
                match *self {
                    $p(ref mut array_data) => Ok(array_data.push(item)),
                    _ => Err(format_err!("type mismatch, array is {}", self.dtype())),
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


#[derive(Debug, Clone)]
pub struct ArrayData<T>(Vec<T>);

impl<T: Send + Sync + Clone> ArrayData<T> {
    pub fn from_vec(xs: Vec<T>) -> Self {
        ArrayData(xs)
    }

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

    // Inner option signifies null
    pub fn get(&self, index: usize) -> Option<Option<&T>> {
        // TODO for now it's wrapped in Some to signifiy
        // that it's always non-null
        self.0.get(index).map(|x| Some(x))
    }

    pub fn push(&mut self, item: T) {
        self.0.push(item);
    }

    pub fn multiply_row(&self, multiple: usize) -> Self {
        let mut res = Vec::new();
        for row in &self.0 {
            for _ in 0..multiple {
                res.push(row.clone());
            }
        }
        ArrayData(res)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// Iterator stuff here

/// Iterator for column types.
pub struct ArrayIterator<'a, T: 'a> {
    values: &'a ArrayData<T>,
    index: usize,
}

impl<'a, T> ArrayIterator<'a, T>
{
    pub fn new(values: &'a ArrayData<T>) -> Self {
        ArrayIterator {
            values: values,
            index: 0,
        }
    }
}

impl<'a, T: 'a + Clone> Iterator for ArrayIterator<'a, T>
    where T: Send + Sync
{
    type Item = Option<&'a T>;

    // Outer option is normal iterator Option:
    // whether or not a value exists.
    // The inner Option is to signify a Null
    fn next(&mut self) -> Option<Option<&'a T>> {
        let res = self.values.get(self.index);
        self.index += 1;
        res
    }
}

pub trait DataTypeIterator<'a, T> {
    fn values(self) -> Result<ArrayIterator<'a, T>, Error>;
}

macro_rules! impl_datatype_iter_for_array {
    ($t:ty, $p: path) => {
        impl<'a> DataTypeIterator<'a, $t> for &'a Array {
            fn values(self) -> Result<ArrayIterator<'a, $t>, Error> {
                match self {
                    $p(ref array_data) => Ok(ArrayIterator::new(array_data)),
                    _ => Err(format_err!("type mismatch, array is {}", self.dtype())),
                }
            }
        }
    };
}


impl_datatype_iter_for_array!(i8, Array::Int8);
impl_datatype_iter_for_array!(i16, Array::Int16);
impl_datatype_iter_for_array!(i32, Array::Int32);
impl_datatype_iter_for_array!(i64, Array::Int64);
impl_datatype_iter_for_array!(u8, Array::UInt8);
impl_datatype_iter_for_array!(u16, Array::UInt16);
impl_datatype_iter_for_array!(u32, Array::UInt32);
impl_datatype_iter_for_array!(u64, Array::UInt64);
impl_datatype_iter_for_array!(f32, Array::Float32);
impl_datatype_iter_for_array!(f64, Array::Float64);
impl_datatype_iter_for_array!(String, Array::Str);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dataframe_basic() {
        let df = DataFrame {
            columns: indexmap!{
                "id".to_owned() => Array::Int8(ArrayData(vec![1,2,3,4,5])),
                "population".to_owned() => Array::Int8(ArrayData(vec![42,22,63,34,53])),
            }
        };
        println!("{:?}", df);
    }

    #[test]
    fn test_dataframe_add_col() {
        let mut df = DataFrame {
            columns: indexmap!{
                "id".to_owned() => Array::Int8(ArrayData(vec![1,2,3,4,5])),
                "population".to_owned() => Array::Int8(ArrayData(vec![42,22,63,34,53])),
            }
        };
        println!("{:?}", df);
        let new_col = df.columns[&"population".to_owned()].apply(|&x: &i8| x-2).unwrap();

        df.add_col("new_col".to_owned(), new_col);
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

    #[test]
    fn test_melt_basic() {
        let mut df = DataFrame {
            columns: indexmap!{
                "id".to_owned() => Array::Int8(ArrayData(vec![1,2,3,4,5])),
                "id2".to_owned() => Array::Int8(ArrayData(vec![6,7,8,9,15])),
                "A".to_owned() => Array::Int8(ArrayData(vec![42,22,63,34,53])),
                "B".to_owned() => Array::Int8(ArrayData(vec![41,21,61,31,51])),
            }
        };

        let df = melt!(
            df=df,
            id_vars=["id", "id2"],
            value_vars=[("A", i8), ("B", i8)],
            value_primitive_type=u8,
            value_type="UInt8",
            var_name="var",
            value_name="value"
            );
        println!("{:?}", df);
        panic!();
    }

    #[test]
    fn test_get() {
        let array = Array::Int8(ArrayData(vec![1,2,3]));
        println!("{:?}", array);
        let x: Result<Option<Option<&i8>>, Error> = array.get(0);
        println!("{:?}", x);
        panic!();
    }
}
