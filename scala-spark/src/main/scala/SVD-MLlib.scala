/**
 * Created by ThirstyTM on 2015-11-18.
 */

//import org.apache.spark.mllib.linalg.Matrix
//import org.apache.spark.mllib.linalg.distributed.RowMatrix
//import org.apache.spark.mllib.linalg.SingularValueDecomposition
//
//
//val mat: RowMatrix = ...
//// Compute the top 20 singular values and corresponding singular vectors.
//val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20, computeU = true)
//val U: RowMatrix = svd.U // The U factor is a RowMatrix.
//val s: Vector = svd.s // The singular values are stored in a local dense vector.
//val V: Matrix = svd.V // The V factor is a local dense matrix