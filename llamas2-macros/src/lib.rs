#![feature(proc_macro)]

extern crate proc_macro;
#[macro_use]
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{Expr, Ident, Type};
use syn::synom::Synom;

#[proc_macro]
pub fn col_get(input: TokenStream) -> TokenStream {
    let ColGet {col, i, ty} = syn::parse(input).expect("failed to parse input");
    let output = quote! {
        #col
            .get(#i)
            .expect("Wrong type")
            .expect(format!("could not find index {} in col", #i).as_str())
            .unwrap() // this second unwrap is because no nulls impl yet
            .clone()
    };
    output.into()
}

// for parsing args to the macro
struct ColGet {
    col: Ident,
    i: Expr,
    ty: Type
}

impl Synom for ColGet {
    named!(parse -> Self, do_parse!(
        col: syn!(Ident) >>
        punct!(,) >>
        i: syn!(Expr) >>
        punct!(,) >>
        ty: syn!(Type) >>
        (ColGet { col, i, ty })
    ));
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
