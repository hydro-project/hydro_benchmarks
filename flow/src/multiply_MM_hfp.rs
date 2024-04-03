use hydroflow_plus::*;
use stageleft::*;

pub fn multiply_MM<'a, D: LocalDeploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
) {

    const M: usize = 3;
    const N: usize = 2;
    const P: usize = 1;

    let process = flow.process(process_spec);

    // Matrix A: m x n
    let matrix_a = process.source_iter(
        q!([[1.0; N] ;M])
    );
    let matrix_b = process.source_iter(
        q!([[1.0; P] ;N])
    );

    let matrix_a_indexed_by_k = matrix_a.enumerate().flat_map(q!(
        |(i, row)| {
            row.into_iter().enumerate().map(
                |(k, a_ik)| {
                    (k, (i, a_ik))
                }
            )
        }
    ));

    let matrix_b_indexed_by_k = matrix_b.enumerate().flat_map(q!(
        |(k, row)| {
            row.into_iter().enumerate().map(
                |(j, b_kj)| {
                    (k, (j, b_kj))
                }
            )
        }
    ));

    // TODO: Join
    let matrix_a_b = matrix_a_indexed_by_k.cross_product(matrix_b_indexed_by_k);

    let matrix_c = matrix_a_b.map(q!(
        |((k, (i, a_ik)), (j, b_kj))| {
            (i, j, a_ik * b_kj)
        }
    ));

    let matrix_c_folded = matrix_c.fold(
        q!(|| vec![vec![0.0; P]; M]),
        q!(
            |acc, (i, j, c_ij)| {
                acc[i][j] += c_ij;
            }
        )
    );

}