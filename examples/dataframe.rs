extern crate failure;
#[macro_use]
extern crate llamas2;
#[macro_use]
extern crate indexmap;

use failure::Error;
use llamas2::dataframe::{Array, ArrayData, DataFrame, DataType};

fn main() {
    test_dataframe_add_col();
    test_melt_basic();
    test_get();
}

fn test_dataframe_add_col() {
    let mut df = DataFrame {
        columns: indexmap!{
            "id".to_owned() => Array::Int8(ArrayData::from_vec(vec![1,2,3,4,5])),
            "population".to_owned() => Array::Int8(ArrayData::from_vec(vec![42,22,63,34,53])),
        }
    };
    println!("{:?}", df);
    let new_col = df.columns[&"population".to_owned()].apply(|&x: &i8| x-2).unwrap();

    df.add_col("new_col".to_owned(), new_col);
    println!("{:?}", df);
}

fn test_melt_basic() {
    let df = DataFrame {
        columns: indexmap!{
            "id".to_owned() => Array::Int8(ArrayData::from_vec(vec![1,2,3,4,5])),
            "id2".to_owned() => Array::Int8(ArrayData::from_vec(vec![6,7,8,9,15])),
            "A".to_owned() => Array::Int8(ArrayData::from_vec(vec![42,22,63,34,53])),
            "B".to_owned() => Array::Int8(ArrayData::from_vec(vec![41,21,61,31,51])),
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
}

fn test_get() {
    let array = Array::Int8(ArrayData::from_vec(vec![1,2,3]));
    println!("{:?}", array);
    let x: Result<Option<Option<&i8>>, Error> = array.get(0);
    println!("{:?}", x);
}

