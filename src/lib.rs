#[macro_use]
extern crate failure;
extern crate rayon;

mod dataframe;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
