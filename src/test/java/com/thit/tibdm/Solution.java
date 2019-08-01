package com.thit.tibdm;


import jnr.ffi.annotations.In;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class Solution {
    private boolean isPalindrome(int x) {
        if (x < 0) {
            return false;
        } else {
            List<Integer> result = new ArrayList<>();
            while (x >= 10) {
                result.add(x % 10);
                x = x / 10;
            }
            result.add(x);
            for (int i = 0; i < result.size() / 2; i++) {
                if (!result.get(i).equals(result.get(result.size() - 1 - i))) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 判断数字回文
     */
    @Test
    public void testIsPalindrome() {
        Assert.assertTrue(isPalindrome(121));
        Assert.assertTrue(!isPalindrome(-121));
        Assert.assertTrue(isPalindrome(11));
        Assert.assertTrue(!isPalindrome(10000));
        Assert.assertTrue(isPalindrome(10001));
        Assert.assertTrue(!isPalindrome(-10001));
    }


    public class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }

    /**
     * 合并两个有序链表
     * 使用递归的方式来实现
     *
     * @param l1
     * @param l2
     * @return
     */
    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if (l1 == null) {
            return l2;
        }
        if (l2 == null) {
            return l1;
        }
        if (l1.val < l2.val) {
            l1.next = mergeTwoLists(l1.next, l2);
            return l1;
        } else {
            l2.next = mergeTwoLists(l1, l2.next);
            return l2;
        }
    }

    public int removeDuplicates(int[] nums) {
        int size = 0;
        for (int i = 0; i < nums.length - 1; i++) {
            if (nums[size] != nums[i + 1]) {
                size++;
                nums[size] = nums[i + 1];
            }
        }
        return size + 1;
    }

    @Test
    public void testRemoveDuplicates() {
        int[] nums = new int[]{1, 1, 2, 2, 3, 4, 5, 6, 7, 7, 8, 8, 9, 9, 10};
        int i = removeDuplicates(nums);
        System.out.println(i);
    }

    @Test
    public void testMergeTwoLists() {
        ListNode listnode1 = new ListNode(1);
        ListNode listnode2 = new ListNode(2);
        ListNode listnode3 = new ListNode(3);
        ListNode listnode4 = new ListNode(8);
        ListNode listnode5 = new ListNode(1);
        ListNode listnode6 = new ListNode(5);
        ListNode listnode7 = new ListNode(8);
        ListNode listnode8 = new ListNode(10);
        listnode3.next = listnode4;
        listnode2.next = listnode3;
        listnode1.next = listnode2;

        listnode7.next = listnode8;
        listnode6.next = listnode7;
        listnode5.next = listnode6;
        ListNode listNode = mergeTwoLists(listnode5, listnode1);
        System.out.println(listNode.toString());
    }


    /**
     * 单链表回文
     *
     * @param head
     * @return
     */
    public boolean isPalindrome(ListNode head) {
        List<Integer> result = new ArrayList<>();
        result.add(head.val);
        while (head.next != null) {
            result.add(head.next.val);
            head = head.next;
        }
        for (int i = 0; i < result.size() / 2; i++) {
            if (!result.get(i).equals(result.get(result.size() - 1 - i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 罗马数字转十进制数字
     *
     * @param s
     * @return
     */
    public int romanToInt(String s) {
        Map<String, Integer> map = new HashMap<>();
        map.put("I", 1);
        map.put("V", 5);
        map.put("X", 10);
        map.put("L", 50);
        map.put("C", 100);
        map.put("D", 500);
        map.put("M", 1000);
        map.put("IV", 4);
        map.put("IX", 9);
        map.put("XL", 40);
        map.put("XC", 90);
        map.put("CD", 400);
        map.put("CM", 900);
        int result = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.length() - i < 2) {
                String oneKey = s.substring(i, i + 1);
                result = map.get(oneKey) + result;
            } else {
                String oneKey = s.substring(i, i + 1);
                String twoKey = s.substring(i, i + 2);
                if (map.containsKey(twoKey)) {
                    result = map.get(twoKey) + result;
                    i++;
                } else {
                    result = map.get(oneKey) + result;
                }
            }
        }
        return result;
    }

    @Test
    public void testRom2Int() {
        String str = "MCMXCIV";
        Assert.assertTrue(1994 == romanToInt(str));
    }

    /**
     * 整形类型转罗马数字
     *
     * @param num
     * @return
     */
    public String intToRoman(int num) {
        StringBuilder sb = new StringBuilder();
        Map<Integer, String> map = new TreeMap<>(Comparator.reverseOrder());
        map.put(1, "I");
        map.put(5, "V");
        map.put(10, "X");
        map.put(50, "L");
        map.put(100, "C");
        map.put(500, "D");
        map.put(1000, "M");
        map.put(4, "IV");
        map.put(9, "IX");
        map.put(40, "XL");
        map.put(90, "XC");
        map.put(400, "CD");
        map.put(900, "CM");

        for (Integer k : map.keySet()) {
            if (num <= 3) {
                for (int i = 0; i < num; i++) {
                    sb.append("I");
                }
                break;
            } else {
                if (num >= k) {
                    int count = num / k;
                    for (int i = 0; i < count; i++) {
                        sb.append(map.get(k));
                    }
                    num = num % k;
                    if (num == 0) {
                        break;
                    }
                }
            }
        }
        return sb.toString();
    }


    /**
     * 最长公共前缀子序列
     *
     * @param strs
     * @return
     */
    public String longestCommonPrefix(String[] strs) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (int i = 0; i < strs.length; i++) {
            if (strs[i].length() == 0) {
                return sb.toString();
            }
            if (count == 0) {
                count = strs[i].length();
            } else {
                if (strs[i].length() < count) {
                    count = strs[i].length();
                }
            }
        }
        lableA:
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < strs.length - 1; j++) {
                if (!strs[j].substring(i, i + 1).equals(strs[j + 1].substring(i, i + 1))) {
                    break lableA;
                }
            }
            sb.append(strs[0], i, i + 1);
        }
        return sb.toString();
    }

    @Test
    public void testLongestCommonPrefix() {
        String[] strings = new String[]{"aa", "a"};
        Assert.assertEquals("a", longestCommonPrefix(strings));
    }

    @Test
    public void testInt2Rom() {
        Stack<String> objects = new Stack<>();
        Assert.assertEquals("XIX", intToRoman(19));
    }

    /**
     * 验证符号的对称
     *
     * @param s
     * @return
     */
    public boolean isValid(String s) {
        boolean result = true;
        Map<String, String> map = new HashMap<>();
        map.put("{", "}");
        map.put("(", ")");
        map.put("[", "]");
        Stack<String> stack = new Stack<>();
        if (s.length() != 0) {
            if (s.length() % 2 == 0) {
                for (int i = 0; i < s.length(); i++) {
                    if (map.containsValue(s.substring(i, i + 1))) {
                        if (stack.size() == 0) {
                            result = false;
                            break;
                        }
                        String pop = stack.pop();
                        if (!map.get(pop).equals(s.substring(i, i + 1))) {
                            result = false;
                            break;
                        }
                    } else {
                        stack.push(s.substring(i, i + 1));
                    }
                }
            } else {
                result = false;
            }
        }
        if (stack.size() != 0 && !map.containsValue(stack.pop())) {
            result = false;
        }
        return result;
    }

    @Test
    public void testIsValid() {
        String test = "(]";
        Assert.assertTrue(!isValid(test));
    }


    /**
     * 返回不常用单词列表
     *
     * @param A
     * @param B
     * @return
     */
    public String[] uncommonFromSentences(String A, String B) {
        List<String> list = new ArrayList<>();
        Map<String, Integer> wordCount = new HashMap<>();
        String[] arrayA = A.split(" ");
        String[] arrayB = B.split(" ");
        wordCount = getWordCount(wordCount, arrayA);
        wordCount = getWordCount(wordCount, arrayB);
        wordCount.forEach((k, v) -> {
            if (v == 1) {
                list.add(k);
            }
        });
        String[] strings = new String[list.size()];
        return list.toArray(strings);
    }

    public Map<String, Integer> getWordCount(Map<String, Integer> wordCount, String[] array) {
        for (int i = 0; i < array.length; i++) {
            wordCount.put(array[i], wordCount.getOrDefault(array[i], 0) + 1);
        }
        return wordCount;
    }

    @Test
    public void testuncommonFromSentences() {
        String A = "this apple is sweet";
        String B = "this apple is sour";
        String[] strings = uncommonFromSentences(A, B);
    }

    /**
     * 开发一个hashset
     */
    class MyHashSet {
        private int count;

        /**
         * 解决hash冲突的链表
         */
        class Node {
            private int value;
            private Node next;

            public Node(int value) {
                this.value = value;
            }
        }

        /**
         * 内置数组
         */
        private Node[] array;

        /**
         * 初始化以及初始化数量
         */
        public MyHashSet() {
            count = 0;
            array = new Node[8];
        }

        public void add(int key) {
            add(key, false);
        }

        public void add(int key, boolean isRise) {
            boolean isEques = false;
            //当容器的总数超过数组大小的4倍的时候进行扩容，一次扩容4倍
            if (count > array.length * 4) {
                reset(4);
            }
            Integer hash = key;
            int index = hash.hashCode() % array.length;
            if (array[index] == null) {
                array[index] = new Node(key);
            } else {
                Node node = array[index];
                Node tmp = array[index];
                while (node != null) {
                    if (node.value == key) {
                        isEques = true;
                        break;
                    } else {
                        if (node.next == null) {
                            break;
                        }
                        node = node.next;
                    }
                }
                if (!isEques) {
                    array[index] = new Node(key);
                    array[index].next = tmp;
                }
            }
            if (!isRise) {
                count++;
            }
        }

        private void reset(double coefficient) {
            Node[] tmp = array;
            this.array = new Node[(int) (tmp.length * coefficient)];
            for (int i = 0; i < tmp.length; i++) {
                Node node = tmp[i];
                if (tmp[i] != null) {
                    while (node != null) {
                        add(node.value, true);
                        if (node.next == null) {
                            break;
                        } else {
                            node = node.next;
                        }
                    }
                }
            }
        }


        /**
         * 移除的化需要进行缩小
         * 数组的大小维持在大于数量的八分之一  小于数量四分之一之间维持一个略微高效的结构
         * 先找到索引位置，然后去找链表，找到之后将节点的下一个的
         *
         * @param key
         */
        public void remove(int key) {
            if (array.length > count * 8 && count != 0) {
                reset(0.25);
            }
            Integer hash = key;
            int index = hash.hashCode() % array.length;
            if (array[index] != null) {
                array[index] = removeNode(array[index], key);
            }
        }

        /**
         * 移除节点
         *
         * @return
         */
        public Node removeNode(Node node, int value) {
            if (node.value == value) {
                node = node.next;
                count--;
            } else {
                if (node.next != null) {
                    node.next = removeNode(node.next, value);
                }
            }
            return node;
        }

        /**
         * Returns true if this set contains the specified element
         */
        public boolean contains(int key) {
            boolean result = false;
            Integer hash = key;
            int index = hash.hashCode() % array.length;
            if (array[index] != null) {
                Node node = array[index];
                while (node != null) {
                    if (node.value == key) {
                        result = true;
                        break;
                    } else {
                        if (node.next == null) {
                            break;
                        }
                        node = node.next;
                    }
                }
            }
            return result;
        }
    }


    @Test
    public void testHashSet() {
        String[] action = new String[]{"remove", "add", "contains", "add", "contains", "add", "add", "add", "add", "contains", "remove", "contains", "contains", "contains", "add", "contains", "add", "remove", "add", "add", "remove", "contains", "add", "contains", "add", "add", "add", "add", "remove", "add", "remove", "add", "remove", "contains", "contains", "add", "add", "add", "add", "add", "contains", "remove", "add", "add", "add", "contains", "contains", "add", "add", "contains", "remove", "add", "contains", "add", "remove", "remove", "add", "contains", "add", "add", "add", "add", "add", "remove", "add", "add", "contains", "contains", "contains", "add", "add", "contains", "contains", "add", "add", "add", "contains", "contains", "add", "add", "remove", "add", "add", "contains", "add", "remove", "add", "contains", "contains", "remove", "remove", "add", "contains", "remove", "contains", "add", "add", "contains", "add", "add"};
        int[] value = new int[]{28, 82, 82, 15, 82, 33, 7, 93, 61, 93, 15, 33, 6, 15, 7, 15, 89, 66, 16, 7, 81, 89, 98, 98, 40, 88, 29, 81, 17, 83, 33, 44, 22, 82, 82, 8, 63, 13, 19, 89, 41, 67, 37, 17, 57, 41, 30, 23, 82, 23, 51, 80, 81, 15, 95, 45, 49, 93, 7, 45, 86, 74, 85, 69, 7, 2, 13, 92, 3, 40, 32, 29, 74, 37, 66, 14, 91, 82, 99, 84, 87, 56, 49, 85, 34, 32, 38, 76, 34, 19, 81, 83, 30, 15, 41, 42, 5, 19, 26, 33};
        String[] result = new String[]{"null", "null", "true", "null", "true", "null", "null", "null", "null", "true", "null", "true", "false", "false", "null", "false", "null", "null", "null", "null", "null", "true", "null", "true", "null", "null", "null", "null", "null", "null", "null", "null", "null", "true", "true", "null", "null", "null", "null", "null", "false", "null", "null", "null", "null", "false", "false", "null", "null", "true", "null", "null", "true", "null", "null", "null", "null", "true", "null", "null", "null", "null", "null", "null", "null", "null", "true", "false", "false", "null", "null", "true", "true", "null", "null", "null", "false", "true", "null", "null", "null", "null", "null", "true", "null", "null", "null", "false", "true", "null", "null", "null", "false", "null", "false", "null", "null", "false", "null", "null"};
        MyHashSet myHashSet = new MyHashSet();
        for (int i = 0; i < action.length; i++) {
            if (action[i].equals("contains")) {
                if (result[i].equals("false")) {
                    Assert.assertTrue(!myHashSet.contains(value[i]));
                } else if (result[i].equals("true")) {
                    Assert.assertTrue(myHashSet.contains(value[i]));
                }
            } else if (action[i].equals("remove")) {
                myHashSet.remove(value[i]);
            } else if (action[i].equals("add")) {
                myHashSet.add(value[i]);
            }
        }
    }

    public int getClimbingWays(int n) {
        if (n < 1) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        if (n == 2) {
            return 2;
        }
        int a = 1;
        int b = 2;
        int temp = 0;
        for (int i = 3; i < n; i++) {
            temp = a + b;
            a = b;
            b = temp;
        }
        return temp;
    }

    /**
     * 硬币找零：动态规划算法
     *
     * @param values     :保存不同硬币的币值的数组
     * @param valueKinds :values数组的大小
     * @param money      :需要找零的面值
     * @param coinsUsed  :保存面值为i的纸币找零所需的最小硬币数
     */
    public static void makeChange(int[] values, int valueKinds, int money,
                                  int[] coinsUsed) {
        coinsUsed[0] = 0;
        // 对所有数额都找零(从小到大)，即保存子问题的解以备用，即填表
        for (int cents = 0; cents <= money; cents++) {
            int minCoins = cents;
            //遍历每一种的面值，看是否可以作为找零的其中之一
            for (int kind = 0; kind < valueKinds; kind++) {
                if (values[kind] <= cents) {
                    int temp = coinsUsed[cents - values[kind]] + 1;
                    if (temp < minCoins) {
                        minCoins = temp;
                    }
                }
            }
            // 保存最小硬币数
            coinsUsed[cents] = minCoins;

            System.out.println("面值为 " + (cents) + " 的最小硬币数 : "
                    + coinsUsed[cents]);
        }
    }

    @Test
    public void testMakeChange() {
        // 硬币面值预先已经按降序排列
        int[] coinValue = new int[]{25, 21, 10, 5, 1};
        // 需要找零的面值
        int money = 63;
        // 保存每一个面值找零所需的最小硬币数，0号单元舍弃不用，所以要多加1
        int[] coinsUsed = new int[money + 1];

        makeChange(coinValue, coinValue.length, money, coinsUsed);
    }


    public class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }
    }

    /**
     * 遍历二叉树
     * 从底到顶
     * 从左到右
     *
     * @param root
     * @return
     */
    public List<List<Integer>> levelOrderBottom(TreeNode root) {
        int hight = 0;
        List<List<Integer>> result = new ArrayList<>();
        if (root != null) {
            List<Integer> tmp = new ArrayList<>();
            tmp.add(root.val);
            result.add(tmp);
            getList(result, root, hight);
        }
        Collections.reverse(result);
        return result;
    }

    public void getList(List<List<Integer>> result, TreeNode treeNode, int hight) {
        if (treeNode.left == null && treeNode.right == null) {
            return;
        }
        hight++;
        List<Integer> tmp = new ArrayList<>();
        if (result.size() > hight) {
            tmp = result.get(hight);
            result.remove(hight);
        }
        result.add(hight, tmp);
        if (treeNode.left != null) {
            tmp.add(treeNode.left.val);
            getList(result, treeNode.left, hight);
        }
        if (treeNode.right != null) {
            tmp.add(treeNode.right.val);
            getList(result, treeNode.right, hight);
        }
    }


    public class TreeLinkNode {
        int val;
        TreeLinkNode left, right, next;

        TreeLinkNode(int x) {
            val = x;
        }
    }


    @Test
    public void testLevelOrderBottom() {
        TreeNode treeNode3 = new TreeNode(3);
        TreeNode treeNode9 = new TreeNode(9);
        TreeNode treeNode20 = new TreeNode(20);
        TreeNode treeNode15 = new TreeNode(15);
        TreeNode treeNode7 = new TreeNode(7);
        TreeNode treeNode8 = new TreeNode(8);
        TreeNode treeNode10 = new TreeNode(10);
        treeNode3.left = treeNode9;
        treeNode3.right = treeNode20;
        treeNode20.left = treeNode15;
        treeNode20.right = treeNode7;
        treeNode9.left = treeNode8;
        treeNode9.right = treeNode10;
        List<List<Integer>> lists = levelOrderBottom(treeNode3);
        System.out.println(lists.toString());
    }
}