use crate::point::Point;

// Function to calculate the Euclidean distance between two points
fn euclidean_distance(point1: &[f64], point2: &[f64]) -> f64 {
    point1.iter()
        .zip(point2.iter())
        .map(|(&x, &y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

// Function to assign each point to the nearest centroid
pub fn assign_points_to_clusters(points: &[Point], centroids: &[Point]) -> Vec<usize> {
    points
        .iter()
        .map(|point| {
            let mut min_distance = f64::MAX;
            let mut cluster_index = 0;

            for (i, centroid) in centroids.iter().enumerate() {
                let distance = euclidean_distance(&point.coordinates, &centroid.coordinates);
                if distance < min_distance {
                    min_distance = distance;
                    cluster_index = i;
                }
            }

            cluster_index
        })
        .collect()
}

// Function to update the centroids based on the assigned points
pub fn update_centroids(points: &[Point], clusters: &[usize], k: usize) -> Vec<Point> {
    let mut centroids = vec![Point {coordinates: vec![0.0; points[0].coordinates.len()]}; k];
    let mut cluster_sizes = vec![0; k];

    for (point, &cluster) in points.iter().zip(clusters.iter()) {
        for (i, &coordinate) in point.coordinates.iter().enumerate() {
            centroids[cluster].coordinates[i] += coordinate;
        }
        cluster_sizes[cluster] += 1;
    }

    for (centroid, &cluster_size) in centroids.iter_mut().zip(cluster_sizes.iter()) {
        for coordinate in centroid.coordinates.iter_mut() {
            *coordinate /= cluster_size as f64;
        }
    }

    centroids
}

// Function to check if the centroids have converged
pub fn has_converged(old_centroids: &[Point], new_centroids: &[Point], tolerance: f64) -> bool {
    old_centroids
        .iter()
        .zip(new_centroids.iter())
        .all(|(old_centroid, new_centroid)| {
            euclidean_distance(&old_centroid.coordinates, &new_centroid.coordinates) <= tolerance
        })
}

// K-means algorithm
pub fn kmeans(points: Vec<Point>, k: usize, max_iterations: usize, tolerance: f64) -> Vec<usize> {
    let mut centroids = points[..k].to_vec();
    let mut clusters = vec![0; points.len()];

    for _ in 0..max_iterations {
        let old_centroids = centroids.clone();
        clusters = assign_points_to_clusters(&points, &centroids);
        centroids = update_centroids(&points, &clusters, k);

        if has_converged(&old_centroids, &centroids, tolerance) {
            break;
        }
    }

    clusters
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let point1 = vec![1.0, 2.0, 3.0];
        let point2 = vec![4.0, 5.0, 6.0];
        let distance = euclidean_distance(&point1, &point2);
        assert_eq!(distance, 5.196152422706632);

        let point1 = vec![0.0, 0.0];
        let point2 = vec![3.0, 4.0];
        let distance = euclidean_distance(&point1, &point2);
        assert_eq!(distance, 5.0);

        let point1 = vec![0.0, 0.0];
        let point2 = vec![-3.0, -4.0];
        let distance = euclidean_distance(&point1, &point2);
        assert_eq!(distance, 5.0);
    }

    #[test]
    fn test_assign_points_to_clusters() {
        let points = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
            Point {coordinates: vec![5.0, 6.0] },
        ];
        let centroids = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![4.0, 5.0] },
        ];
        let clusters = assign_points_to_clusters(&points, &centroids);
        assert_eq!(clusters, vec![0, 1, 1]);
    }

    #[test]
    fn test_update_centroids() {
        let points = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
            Point {coordinates: vec![5.0, 6.0] },
        ];
        let clusters = vec![0, 1, 1];
        let k = 2;
        let new_centroids = update_centroids(&points, &clusters, k);
        assert_eq!(new_centroids[0].coordinates, vec![1.0, 2.0]);
        assert_eq!(new_centroids[1].coordinates, vec![4.0, 5.0]);
    }

    #[test]
    fn test_has_converged() {
        let old_centroids = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
        ];
        let new_centroids = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
        ];
        let tolerance = 0.1;
        let converged = has_converged(&old_centroids, &new_centroids, tolerance);
        assert_eq!(converged, true);

        let old_centroids = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
        ];
        let new_centroids = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![5.0, 6.0] },
        ];
        let tolerance = 0.1;
        let converged = has_converged(&old_centroids, &new_centroids, tolerance);
        assert_eq!(converged, false);
    }

    #[test]
    fn test_kmeans() {
        let points = vec![
            Point {coordinates: vec![1.0, 2.0] },
            Point {coordinates: vec![3.0, 4.0] },
            Point {coordinates: vec![5.0, 6.0] },
        ];
        let k = 2;
        let max_iterations = 10;
        let tolerance = 0.1;
        let clusters = kmeans(points, k, max_iterations, tolerance);
        assert_eq!(clusters, vec![0, 1, 1]);
    }

    #[test]
    fn test_kmeans_multiple_iterations() {
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
        let clusters = kmeans(points, k, max_iterations, tolerance);
        assert_eq!(clusters, vec![0, 0, 1, 1, 1]);
    }
}
