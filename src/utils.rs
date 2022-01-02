#[macro_export]
macro_rules! max {
    ($x: expr) => ($x);
    ($x: expr, $($z: expr),+) => (::std::cmp::max($x, max!($($z),*)));
}
