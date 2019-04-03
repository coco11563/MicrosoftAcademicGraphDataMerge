package test;

import pub.sha0w.ETL.graphProcess.obj.Venue;

import java.io.*;

public class SerializableDemo1 {
    public static void main(String[] args) throws Exception, IOException {
        //初始化对象
        Venue user = new Venue("1","2");
        System.out.println(user);
        //序列化对象到文件中
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("template"));
        oos.writeObject(user);
        oos.close();
        //反序列化
        File file = new File("template");
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
        Venue newUser = (Venue)ois.readObject();
        System.out.println(newUser);
        ois.close();
    }
}
