use hydroflow_base::kmeans_hf::kmeans_hf;
use base::point::*;

fn main() {
    let points = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
            Point {coordinates: vec![5.0, 6.0] },
            Point {coordinates: vec![7.0, 8.0] },
            Point {coordinates: vec![9.0, 10.0] },
        ];
    let k = 2;
    let tolerance = 0.1;
    let max_iterations = 100;

    let res = kmeans_hf(points, k, max_iterations, tolerance);

    println!("Result: {:?}", res);
}