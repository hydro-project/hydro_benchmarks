stageleft::stageleft_crate!(flow_macro);

#[cfg(stageleft_macro)]
pub(crate) mod first_ten;
#[cfg(not(stageleft_macro))]
pub mod first_ten;

#[cfg(stageleft_macro)]
pub(crate) mod first_ten_distributed;
#[cfg(not(stageleft_macro))]
pub mod first_ten_distributed;

#[cfg(stageleft_macro)]
pub(crate) mod tpch;
#[cfg(not(stageleft_macro))]
pub mod tpch;

//pub mod kmeans_shallow_hfp;
//pub mod multiply_MM_hfp;