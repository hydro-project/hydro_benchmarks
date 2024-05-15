use std::time::Duration;

use hydroflow::hydroflow_syntax;
use chrono::{DateTime, Local, TimeDelta};
use hydroflow::util::Persistence::*;

/**
 * A simple sliding window implementation.
 * This is a simple example of a single sliding window over incoming items in HydroFlow.
 */
pub async fn simple_sliding_window() {

    let input_delay = Duration::from_millis(1000);
    let slice = Duration::from_secs(1);
    let window_size = TimeDelta::milliseconds(500);

    println!("Sliding window of size {:?} every {:?}", window_size, slice);

    let input = vec![1, 2, 3, 4];

    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<i32>();

    let mut flow = hydroflow_syntax! {

        // The window 
        window = union()
            -> inspect(|x| {
                let tick = context.current_tick();
                match x {
                    Delete(x) => {
                        println!("Deleting {:?} in tick {}", x, tick);
                    }
                    Persist(x) => {
                        println!("Persisting {:?} in tick {}", x, tick);
                    }
                }
            })
            -> persist_mut();

        // Tag incoming items with arrival time and persist in window
        input = source_stream(input_recv)
            -> map(|x| (x, Local::now()))
            -> map(|x| Persist(x))
            -> window;

        // Delete dated items from window
        window_out[delete]
            -> map(|(x, _)| Delete(x))
            -> defer_tick()
            -> window;
        
        // Annotate the items with the window end time
        window_end = source_interval(slice) -> map(|_| Local::now());
        window -> [0]window_out;
        window_end -> [1]window_out;
        
        // Delete items older than window_size
        /* 
        window_out = cross_join_multiset() -> tee();
        window_out
            -> filter(|x| {
                let arrival = x.0.1;
                let end = x.1;
                let duration = arrival.signed_duration_since(end);
                //println!("Duration {:?}", duration);
                duration < -window_size
            })
            -> map(|(x, _)| Delete(x))
            -> defer_tick()
            -> window; */

        // Separate the items into items of current window and dated items to be deleted
        window_out = cross_join_multiset() -> partition(|x: &((i32, DateTime<Local>), DateTime<Local>), [out, delete]| {
                let arrival = x.0.1;
                let end = x.1;
                let duration = end.signed_duration_since(arrival);
                let is_old = duration > window_size;
                println!("Duration {:?} is old: {}", duration, is_old);
                match is_old {
                    true => delete,
                    false => out
                }
            });

        // Process the items in the current window
        window_out[out] -> for_each(|x| {
            let tick = context.current_tick();
            println!("Received {:?} in tick {}", x, tick);
        });

    };

    println!("Starting");

    for i in input {
        input_send.send(i).unwrap();
        flow.run_available_async().await;
        tokio::time::sleep(input_delay).await;
    }
    
}

/**
 * Concurrent sliding windows
 */
pub async fn sliding_window() {
    todo!("Implement concurrent sliding windows");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_sliding_window() {
        simple_sliding_window().await;
    }

    #[tokio::test]
    async fn test_sliding_window() {
        sliding_window().await;
    }
}