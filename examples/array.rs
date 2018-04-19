extern crate llamas2;

use llamas2::dataframe::{Array, ArrayData, DataType};

fn main() {
    test_array_apply_inplace();
    test_array_apply();
    test_array_apply_wrong_type();
}


fn test_array_apply_inplace() {
    let mut array = Array::Int8(ArrayData::from_vec(vec![1,2,3]));
    array.apply_inplace(|x: &mut i8| *x = *x*2).unwrap();
    println!("{:?}", array);
    fn test_fn(x: &mut i8) {
        *x = x.pow(2);
    }
    array.apply_inplace(test_fn).unwrap();
    println!("{:?}", array);
}

fn test_array_apply() {
    let array = Array::Int8(ArrayData::from_vec(vec![1,2,3]));
    let array1 = array.apply(|&x: &i8| x*2).unwrap();
    println!("{:?}", array1);
    fn test_fn1(x: &i8) -> i8 {
        x.pow(2)
    }

    let array2 = array.apply(test_fn1).unwrap();
    println!("{:?}", array2);

    let array = Array::UInt8(ArrayData::from_vec(vec![1,2,3]));
    let array1 = array.apply(|&x: &u8| x*2);
    println!("{:?}", array1);
    fn test_fn2(x: &u8) -> u8 {
        x.pow(2)
    }

    let array2 = array.apply(test_fn2).unwrap();
    println!("{:?}", array2);
}

fn test_array_apply_wrong_type() {
    let array = Array::UInt8(ArrayData::from_vec(vec![1,2,3]));
    // Test to see if this compiles
    fn test_fn_bad2(x: &i8) -> i8 {
        x.pow(2)
    }
    array.apply(test_fn_bad2).unwrap();
}

