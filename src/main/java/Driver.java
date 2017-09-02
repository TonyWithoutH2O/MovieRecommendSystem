public class Driver {
    public static void main(String[] args) throws  Exception {
        //args
        String rawData = args[0];
        String dataDivedByUserOutput = args[1];
        String cMatrixOutput = args[2];
        String normalizeResult = args[3];
        String movieList = args[4];
        String rating = args[5];
        String multiplyResult = args[6];
        String addResult = args[7];
        //new object
        DataDividByUser step1 = new DataDividByUser();
        CoMatrix step2 = new CoMatrix();
        Normalize step3 = new Normalize();
        GetMovieList step4 = new GetMovieList();
        GetRatingList step5 = new GetRatingList();
        Multiply step6 = new Multiply();
        Add step7 = new Add();
        //run
        String[] step1Args = {rawData,dataDivedByUserOutput};
        step1.main(step1Args);

        String[] step2Args = {dataDivedByUserOutput, cMatrixOutput};
        step2.main(step2Args);

        String[] step3Args = {cMatrixOutput, normalizeResult};
        step3.main(step3Args);

        String[] step4Args = {rawData, movieList};
        step4.main(step4Args);

        String[] step5Args = {rawData,"hdfs://hadoop-master:9000" + movieList + "/part-r-00000", rating};
        step5.main(step5Args);

        String[] step6Args = {normalizeResult, rating, multiplyResult};
        step6.main(step6Args);

        String[] step7Args= {multiplyResult, addResult};
        step7.main(step7Args);
    }
}

/*

 */