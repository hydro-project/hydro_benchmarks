use hydroflow::hydroflow_syntax;
use hydroflow::scheduled::graph::Hydroflow;
use base::kmeans_baseline::*;
use base::point::*;

pub fn kmeans_hf(points: Vec<Point>, k: usize, _max_iterations: usize, tolerance: f64) -> Vec<usize> {

    const MAX_ITERATIONS: i32 = 1;

    //println!("Running kmeans_hf with k = {}, tolerance = {}", k, tolerance);

    let centroids_init = [points[0..k].to_vec()];

    //let (output_send, mut _output_recv) = hydroflow::util::unsync::mpsc::channel::<Vec<usize>>(None);
    
    let mut flow: Hydroflow = hydroflow_syntax! {
        // Input points
        points = source_iter([points]);

        centroids_init = source_iter(centroids_init);

        // Centroids
        centroids = union();
        centroids_init -> centroids;

        // Collect all points for clustering
        points_folded = points -> persist(); // -> fold::<'tick>(Vec::<Point>::new, |acc, point| { acc.push(point); });

        // Combine points and centroids for input into clustering
        points_folded -> [0]points_centroids;
        centroids -> [1]points_centroids;
        points_centroids =  cross_join_multiset() //zip()
            -> assert(|(_, centroids)| centroids.len() > 0)
            -> assert(|(points, _)| points.len() > 0);
            //-> inspect(|(points, centroids)| println!("Input = Points: {:?}, Centroids: {:?}", points, centroids));

        // Assign points to clusters
        new_centroids_and_clusters = points_centroids -> map(|(points, centroids)| {
                let clusters = assign_points_to_clusters(&points, &centroids);
                let centroids_new = update_centroids(&points, &clusters, k);

                (centroids, centroids_new, clusters)
            });

        // Get new and old centroids and compare
        has_converged = new_centroids_and_clusters
            -> map(|(old_centroids, new_centroids, clusters)| {
                let res = has_converged(&old_centroids, &new_centroids, tolerance);
                (res, new_centroids, clusters)
            });
            //-> inspect(|x| println!("Converged: {:?} (tick {:?})", x, context.current_tick()));

        // Either return or iterate depending on convergence
        // TODO: Implement max_iterations per tick? Requires to remove defer_tick
        // TODO: Implement variable number of iterations per tick
        iteration_end = has_converged
            -> enumerate::<'static>()
            -> inspect(|(iteration, (has_converged, _, _))| println!("Iteration: {:?}, Converged: {:?}", iteration, has_converged))
            -> partition(|(iteration, (has_converged, _, _)): &(i32, (bool, _, _)), [result, iterate]| {
            match ((*iteration)+1, has_converged) {
                (_, true) => result,
                (MAX_ITERATIONS, false) => result,
                (_, false) => iterate,
            }
        });

        // XXX: Partition causes compilation error when subsequent operator is incompatible!
        centroids_new = iteration_end[iterate] -> map(|(_ ,(_, centroids, _))| centroids);
        // XXX: Why is defer tick necessary to create new matches in the cross_join?
        centroids_new -> defer_tick() -> centroids;
        clusters = iteration_end[result] -> map(|(_, (_, centroids, clusters))| (centroids, clusters));

        clusters
            -> inspect(|(centroids, clusters)| println!("Centroids: {:?}, Clusters: {:?}", centroids, clusters))
            -> map(|(_, clusters)| clusters)
            //-> dest_sink(output_send);
            -> null();
    };

    // run the server
    flow.run_available();
    // TODO: receive output
    let res = vec![0, 1, 2];

    return res;
}