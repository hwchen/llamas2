#[macro_use]
extern crate failure;
#[macro_use]
extern crate indexmap;
extern crate rayon;

pub mod dataframe;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
