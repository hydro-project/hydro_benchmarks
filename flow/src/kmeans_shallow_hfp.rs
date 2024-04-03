use hydroflow_plus::*;
use stageleft::*;

use crate::__staged::kmeans_baseline::*;
use crate::__staged::point::*;

pub fn kmeans_shallow<'a, D: LocalDeploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    k: usize,
    tolerance: f64,
) {
    let process = flow.process(process_spec);

    let points = process.source_iter(q!(vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]]))
        .enumerate()
        .map(q!(|(id, coordinates)| {Point { id, coordinates }}
        ));

    let centroids_init = process.source_iter(q!(vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]]))
        .enumerate()
        .map(q!(|(id, coordinates)| {Point { id, coordinates }}
        ));   

    let centroids = centroids_init.union(new_centroids).delta(); //.delta().tee(); //.union()

    let (centroids_completed, centroids_cycle) = process.cycle();
    let centroids = centroids_init.union(centroids_cycle);

    // TODO: Make points persistent or fold static
    let points_folded = points.fold(
        q!(Vec::<Point>::new),
        q!(|acc, point| {
            acc.push(point);
        })
    );

    let points_centroids = points_folded.cross_product(centroids);
    
    let new_centroids_and_clusters = points_centroids.map(q!(
        |(points, centroids)| {
            //let clusters = assign_points_to_clusters(&points, &centroids);
            //let centroids_new = update_centroids(points, &clusters, k);

            let clusters = vec![0, 1, 2];
            let centroids_new = centroids.clone();

            (centroids, centroids_new, clusters)
        }
    ));

    // Get new and old centroids and compare
    let has_converged = new_centroids_and_clusters.map(q!(
        |(old_centroids, new_centroids, clusters)| {
            //let res = has_converged(&old_centroids, &new_centroids, tolerance);
            let res = true;
            (res, old_centroids, new_centroids, clusters)
        }
    )).tee().defer_tick();

    // If not converged, update centroids
    let new_centroids = has_converged.filter(q!(|(res, _, _, _)| !*res)).map(q!(|(_, _, new_centroids, _)| new_centroids));

    // If converged, return clusters
    let clusters = has_converged.filter(q!(|(res, _, _, _)| *res)).map(q!(|(_, _, _, clusters)| clusters));

}