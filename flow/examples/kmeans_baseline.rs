use flow::kmeans_baseline::kmeans;

fn main() {
    // Example usage
    let points = vec![
        vec![1.0, 2.0],
        vec![2.0, 1.0],
        vec![3.0, 4.0],
        vec![4.0, 3.0],
        vec![5.0, 6.0],
        vec![6.0, 5.0],
    ];

    let k = 2;
    let max_iterations = 100;
    let tolerance = 0.0001;

    let clusters = kmeans(&points, k, max_iterations, tolerance);

    for (i, cluster) in clusters.iter().enumerate() {
        println!("Point {:?} belongs to cluster {}", points[i], cluster);
    }
}