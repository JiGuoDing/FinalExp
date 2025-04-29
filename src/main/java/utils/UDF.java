package utils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/*
The UDF Class includes some functions that defined by JGD.
 */
public class UDF {
    /*
    计算两个集合的 Jaccard 系数
     */
    private static double JaccardDistance(Set<?> set1, Set<?> set2){
        /*
        定义当两个集合均为空集时，Jaccard 系数为0
         */
        if (set1.isEmpty() && set2.isEmpty()) {
            return 0;
        }
        /*
        计算交集
         */
        Set<Object> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);
        int intersectionSize = intersection.size();

        /*
        计算并集
         */
        Set<Object> union = new HashSet<>(set1);
        union.addAll(set2);
        int unionSize = union.size();

        /*
        计算 Jaccard 系数
         */
        return (double) intersectionSize / unionSize;
    }
}
