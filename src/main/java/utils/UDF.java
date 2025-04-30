package utils;

import java.util.*;
import java.util.stream.Collectors;

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

    /*
    将包含一个或多个作者的字符串映射到作者 Set
     */
    private static Set<String> parseAuthors2Set(String authorsStr){
        return Arrays.stream(authorsStr.split(",")).map(String::trim).filter(name -> !name.isEmpty()).collect(Collectors.toSet());
    }

    /*
    计算两本图书（Book）的相似度
    参数定义：
        book1：图书1
        book2：图书2
        tagCntList1：图书1的标签列表
        tagCntList2：图书2的标签列表
        cooccurrence：图书1和图书2的共现次数
        maxYearGap：最大年代差值
        userNum：用户总数量
     */
    public static double calSimilarityOf2Book(Book book1, Book book2, List<TagCnt> tagCntList1, List<TagCnt> tagCntList2, int cooccurrence, int maxYearGap, int userNum){
        /*
        计算两本图书的共现相似度
         */
        double cooccurrenceSimilarity = (double) cooccurrence / userNum;

        /*
        计算两本图书的标签相似度
         */
        Set<String> tagsSet1 = tagCntList1.stream().map(TagCnt::getTag_id).collect(Collectors.toSet());
        Set<String> tagsSet2 = tagCntList2.stream().map(TagCnt::getTag_id).collect(Collectors.toSet());

        double tagsSimilarity = JaccardDistance(tagsSet1, tagsSet2);

        /*
        获取两本图书的作者信息
        初始作者字符串格式："author1, author2,..."
         */
        String authors1 = book1.getAuthors();
        String authors2 = book2.getAuthors();

        Set<String> authorsSet1 = parseAuthors2Set(authors1);
        Set<String> authorsSet2 = parseAuthors2Set(authors2);

        double authorsSimilarity = JaccardDistance(authorsSet1, authorsSet2);
        /*
        获取两本图书的年代信息
         */
        String decade1 = book1.getOriginal_publication_decade();
        String decade2 = book2.getOriginal_publication_decade();
        int intDecade1 = Integer.parseInt(decade1);
        int intDecade2 = Integer.parseInt(decade2);

        double decadeSimilarity = 1.0 - (double) (Math.abs(intDecade1 - intDecade2)) / maxYearGap;

        return (cooccurrenceSimilarity + tagsSimilarity + authorsSimilarity + decadeSimilarity) / 4;
    }

    public static void main(String[] args) {
        System.out.println(parseAuthors2Set("A.C. John Smith, JGD, WWH, JK. Rolling"));
        Set<String> set1 = parseAuthors2Set("A.C. John Smith, JGD, WWH, JK. Rolling");
        Set<String> set2 = parseAuthors2Set("WHT, O.G. Rock, GR, WWH");
        System.out.println(JaccardDistance(set1, set2));
    }
}
