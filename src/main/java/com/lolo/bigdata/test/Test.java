package com.lolo.bigdata.test;

public class Test {
    public static void main(String[] args) {
        //System.out.println(User.age);
        //System.out.println(Emp.age);  // 没有执行静态代码块

        A a = new C();
        System.out.println(C.class.getInterfaces().length);
    }
}

interface A {

}

class B implements A {

}

class C extends B {

}
