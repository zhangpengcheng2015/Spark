package zhang.data_algorithms_book.chapter07;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Some methods for FindAssociationRules class.
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        List<String> list = new ArrayList<String>();
        for (String item : items) {
            list.add(item);
        }
        return list;
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || (list.isEmpty())) {
            return list;
        }
        //
        if ((i < 0) || (i > (list.size() - 1))) {
            return list;
        }
        //
        List<String> cloned = new ArrayList<String>(list);
        cloned.remove(i);
        //
        return cloned;
    }

    /*
    *删除文件夹
    * param folderPath 文件夹完整绝对路径
    * */
    static void delFolder(String folderPath) {
        try {
            delAllFile(folderPath); //删除完里面所有内容
            String filePath = folderPath;
            filePath = filePath.toString();
            java.io.File myFilePath = new java.io.File(filePath);
            myFilePath.delete(); //删除空文件夹
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static boolean delAllFile(String path) {
        boolean flag = false;
        File file = new File(path);
        if (!file.exists()) {
            return flag;
        }
        if (!file.isDirectory()) {
            return flag;
        }
        String[] tempList = file.list();
        File temp = null;
        for (int i = 0; i < tempList.length; i++) {
            if (path.endsWith(File.separator)) {
                temp = new File(path + tempList[i]);
            } else {
                temp = new File(path + File.separator + tempList[i]);
            }
            if (temp.isFile()) {
                temp.delete();
            }
            if (temp.isDirectory()) {
                delAllFile(path + "/" + tempList[i]);//先删除文件夹里面的文件
                delFolder(path + "/" + tempList[i]);//再删除空文件夹
                flag = true;
            }
        }
        return flag;
    }
}


