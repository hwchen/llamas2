# llamas 2

## Notes
Second try at a dataframe. llamas 1 got bogged down in traits land.

However, after looking at the datafusion code, I realized that using
enums (with code generation by macros) might be easier to implement,
and have a cleaner interface, than using traits.

Implementation is going faster than before. My objective is to first
implement my everday needs for data cleaning:

- num and string arrays with nulls
- adding cols to df
- melting and pivoting
- splitting strings
- reading/writing csv
- sql table creation fn

Right now:
- df display, finish melt
- in `multiply_row`, first allocate the full vec since we know len and multiple.
- in melt, create a fn returning Result internally to enable using `try?`.
- create extend method for array?
- read csv (to make better tests)

- create Nullable enum?

The best discovery so far is that in enums, trying to pass in the
wrong type of fn for a map/apply will result in a compile-time error!
For some reason I had originally thought that this kind of checking with
generics would only work with traits, but I was wrong.

Whoops, maybe enums don't work after all, trying to dispatch based on enum
variant and fn sig doesn't seem to work so well. Going to try again
with traits, but this time with overloading traits:


[link](https://www.reddit.com/r/rust/comments/7zrycu/so_function_overloading_is_part_of_stable_rust/)

For blogpost, tricky spots:

- getting the dynamic dispatch while also being able to access the primitive type for apply, get.
- getting an iterator from col (similar issue to above)
- melting, how to be able to set type of values col
- melting, getting multiplier (have to create from previous col, in `pub(crate)` method)

## Motivation

My work uses pandas for etl, which mainly consists of reshaping tables. I would love to be able to use Rust so that I don't have to use Python, which can give me headaches.

Since etl is the major usecase, the focus areas are:

- reshaping (melting and pivoting)
- splitting string cols
- apply (or map to dict)
- group by
- filtering
- reading and writing csv (perhaps compressed also)
- generating sql for creating tables
- (performance and ergonomics of course)

non-focus areas:

- numerical computing (but maybe in the future)
- operations on single rows
- ergonomics to python/pandas ease of use

## Design/Influences

This project is most influence by my time using pandas for etl. There are some pandas idiosyncracies (like non-nullable integers!) that I would love to resolve. In that vein, I've been following the development of pandas 2 closely. I'm inspired by the project's focus on performance and ergonomics, and the use of C++ data structures on the backend.

In particular, I want to be able to have an Array representation which combines a null bitvec representation with an Vec of a primitive type or struct. In order to have the most compact representation (and the best alignment?), I'm trying to design this to not have each value be stored as an enum, like some other libraries.

I've seen e.g. `InnerType::Float(x)` in Utah, or `Nullable::Value(T)` and `Nullable::Null` in brassfibres. In addition to both not having the most compact representation, the usage of `InnerType` would seem to allow the use of mixed types within a series, which I would not want to allow.

My other influence is from databases. At work we use columnar database (Monet in particular) as a backend to an OLAP service. And my desire to learn more about databases also led me to [bradfield](https://bradfieldcs.com), where I took a computer architecture and a databases course. As the project for the databases course, I also wrote a toy sql database executor in Rust [link](https://github.com/hwchen/lemurdb).

## Other dataframe/etl projects

- [datafusion](https://github.com/datafusion-rs/datafusion-rs) has a dataframe-like representation, but is meant to be used with sql and query planner on the frontend.
- [brassfibres](https://github.com/sinhrks/brassfibre)
- [utah](https://github.com/kernelmachine/utah)
