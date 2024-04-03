use __staged::ir::HashMap;
use hydroflow_plus::*;
use stageleft::*;

// Euclidean distance in hf?
fn euclidean_distance(point1: &[f64], point2: &[f64]) -> f64 {
    point1.iter()
        .zip(point2.iter())
        .map(|(&x, &y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

#[derive(Debug, Clone)]
struct Point {
    pub id: usize,
    pub coordinates: Vec<f64>,
}

use std::hash::{Hash, Hasher};

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// Assignment of points to clusters
pub fn assign_points_to_clusters<'a, D: LocalDeploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
) {
    let process = flow.process(process_spec);

    let points = process.source_iter(q!(vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]]))
        .enumerate()
        .map(q!(|(id, coordinates)| {Point { id, coordinates }}
        ));

    let centroids = process.source_iter(q!(vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]]))
        .enumerate()
        .map(q!(|(id, coordinates)| {Box::new(Point { id, coordinates })}
        ));

    // cross_join points and centroids
    let points_centroids = points.cross_product(centroids);

    // Map to calculate distance
    let distance = points_centroids.map(q!(
        |(point, centroid)| {
            let distance = point.coordinates.iter()
                .zip(centroid.coordinates.iter())
                .map(|(&x, &y)| (x - y) * (x - y))
                .sum::<f64>()
                .sqrt();
            (point, centroid, distance)
        }
    ));

    // Keyed reduce per point to closest centroid
    let closest_centroid = distance.fold(
        q!(HashMap::<Point, (f64, Box<Point>)>::new),
        q!(
            |acc: &mut HashMap<Point, (f64, Box<Point>)>, (point, centroid, distance)| {
                
                if let Some((closest_distance, ref mut closest_centroid)) = acc.get_mut(&point) {
                    if distance < *closest_distance {
                        *closest_distance = distance;
                        *closest_centroid = centroid;
                    }
                } else {
                    acc.insert(point, (distance, centroid.clone()));
                }
            }
        )
    );

}

// Update centroids
// Assignment of points to clusters
pub fn update_centroids<'a, D: LocalDeploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
) {
    let process = flow.process(process_spec);

    // Aggregate count and sum of coordinates per cluster

    // Map to calculate new centroid

    // Reduce to calculate new centroid
}