## llamas 2

Second try at a dataframe. llamas 1 got bogged down in traits land.

However, after looking at the datafusion code, I realized that using
enums (with code generation by macros) might be easier to implement,
and have a cleaner interface, than using traits.

Implementation is going faster than before. My objective is to first
implement my everday needs for data cleaning:

- num and string arrays with nulls
- assignment of an Array (Series in pandaland) to a Column
- melting and pivoting
- splitting strings
- reading/writing csv
- sql table creation fn
- df display

The best discovery so far is that in enums, trying to pass in the
wrong type of fn for a map/apply will result in a compile-time error!
For some reason I had originally thought that this kind of checking with
generics would only work with traits, but I was wrong.

Whoops, maybe enums don't work after all, trying to dispatch based on enum
variant and fn sig doesn't seem to work so well. Going to try again
with traits, but this time with overloading traits:


[link](https://www.reddit.com/r/rust/comments/7zrycu/so_function_overloading_is_part_of_stable_rust/)
