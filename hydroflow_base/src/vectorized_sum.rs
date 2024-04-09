use hydroflow::hydroflow_syntax;

pub fn vectorized_sum_iterator(a: Vec<f32>) -> f32 {

    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<f32>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        a = source_iter(a);

        // Combine columns and vector elements
        result = a
            -> reduce(|acc: &mut f32, x: f32| {
                *acc += x;
            });

        result
            -> for_each(|x| output_send.send(x).unwrap());
    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<f32>,_>(output_recv);

    assert!(res.len() == 1);
    return res[0].clone();
}

pub fn vectorized_sum_iterator_chunks<const CHUNK_SIZE: usize>(a: Vec<f32>) -> f32 {

    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<f32>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        a = source_iter(a.chunks(CHUNK_SIZE));

        // Combine columns and vector elements
        result = a
            -> map(|chunk| chunk.iter().sum::<f32>())
            -> reduce(|acc: &mut f32, x: f32| {
                *acc += x;
            });

        result
            -> for_each(|x| output_send.send(x).unwrap());
    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<f32>,_>(output_recv);

    assert!(res.len() == 1);
    return res[0].clone();
}

pub fn vectorized_sum_iterator_batching<const CHUNK_SIZE: usize>(a: Vec<f32>) -> f32 {

    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<f32>();
    let mut buf: [f32; CHUNK_SIZE] = [0.0; CHUNK_SIZE];


    let mut flow = hydroflow_syntax! {
        // Inputs
        a = source_iter(a);

        // Combine columns and vector elements
        result = a -> enumerate()
            -> map(move |(i,f)|{
                buf[i%CHUNK_SIZE] = f;
                if i%CHUNK_SIZE == CHUNK_SIZE-1 {
                    Some(buf.clone())
                } else {
                    None
                }
            })
            -> filter(|x| x.is_some())
            -> map(|x| x.unwrap())
            -> map(|chunk| chunk.iter().sum::<f32>())
            -> reduce(|acc: &mut f32, x: f32| {
                *acc += x;
            });
        

        result
            -> for_each(|x| output_send.send(x).unwrap());
    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<f32>,_>(output_recv);

    assert!(res.len() == 1);
    return res[0].clone();
}

pub fn vectorized_sum_iterator_batched<const CHUNK_SIZE: usize>(a: Vec<[f32; CHUNK_SIZE]>) -> f32 {

    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<f32>();

    let mut flow = hydroflow_syntax! {
        // Inputs
        a = source_iter(a);

        // Combine columns and vector elements
        result = a
            -> map(|chunk| chunk.iter().sum::<f32>())
            -> reduce(|acc: &mut f32, x: f32| {
                *acc += x;
            });

        result
            -> for_each(|x| output_send.send(x).unwrap());
    };

    flow.run_available();
    let res = hydroflow::util::collect_ready::<Vec<f32>,_>(output_recv);

    assert!(res.len() == 1);
    return res[0].clone();
}