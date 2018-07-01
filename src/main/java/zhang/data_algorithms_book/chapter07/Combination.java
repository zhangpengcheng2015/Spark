package zhang.data_algorithms_book.chapter07;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;



public class Combination {

    /*
    * 返回所有不同大小的组合...
    * 例，如果elements = {a,b,c},则findCollection(elements)会返回：
    * {[],[a],[b],[c],[a,b],[a,c],[b,c],[a,b,c]}
    *
    * */
    public static <T extends Comparable<? super T>> List<List<T>>
        findSortedCombinations(Collection<T> elements){
        List<List<T>> result = new ArrayList<List<T>>();
        for(int i = 0;i<=elements.size();i++){
            result.addAll(findSortedCombinations(elements,i));
        }

        return result;
    }

    /*
     * 如果elements = {a,b,c},则findCollections(elements,2)会返回:
     * {[a,b],[a,c],[b,c]}
     *
     * */

    //利用迭代
    public static <T extends Comparable<? super T>> List<List<T>>
        findSortedCombinations(Collection<T> elements,int n){
            List<List<T>> result = new ArrayList<List<T>>();
            if(n == 0){
                result.add(new ArrayList<T>());
                return result;
            }
            List<List<T>> combinations = findSortedCombinations(elements,n-1);

            for(List<T> combination:combinations){
                for(T element:elements){
                    //去重
                    if(combination.contains(element))continue;

                    List<T> list = new ArrayList<T>();
                    list.addAll(combination);

                    if(list.contains(element))continue;

                    list.add(element);

                    //对项排序，以避免重复项，例如，如果不排序(a,b,c)和
                    // (a,c,b)可能会记为不同的项
                    Collections.sort(list);
                    if(result.contains(list)){
                        continue;
                    }

                    result.add(list);
                }
            }

            return result;
    }
}
