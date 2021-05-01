package im.yanchen.pupgrowth.USpan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Yuting Yang
 * @create 2021-04-23-22:18
 */
public class GTreeNode implements Serializable {
    public int item;
    // the total utility in local partition
    public int totalUtility;
    public int utilityUp;
    public int gUtilityUp = 0;
    //children info
    List<GTreeNode> children = new ArrayList<GTreeNode>();
    // -1/its number
    public ArrayList<Integer> childrenArray = new ArrayList<Integer>();
    //		public int[] childrenArray;
    public int NumOfChildren = 0;
    // whether is L_HUSP
    Boolean isPromising = false;

    // three kinds of constructors
    // the constructor of the root
    public GTreeNode() {
        this.item = 0;
//            this.childrenArray = new int[20000];
//            Arrays.fill(childrenArray, -1);
    }

    // the constructor of item -1
    public GTreeNode(int item) {
        this.item = -1;
//            this.childrenArray = new int[20000];
//            Arrays.fill(childrenArray, -1);
    }

    public GTreeNode(GTreeNode lnode) {
        this.item = lnode.item;
        this.totalUtility = lnode.totalUtility;
        this.utilityUp = lnode.utilityUp;
        this.NumOfChildren = lnode.NumOfChildren;
        this.childrenArray.addAll(lnode.childrenArray);
        this.isPromising = lnode.isPromising;
        this.gUtilityUp = lnode.gUtilityUp;

        for (GTreeNode node : lnode.children) {
            this.children.add(new GTreeNode(node));
        }
    }

    // the constructor of item
    public GTreeNode(TreeNode lnode) {
        this.item = lnode.item;
        this.totalUtility = lnode.totalUtility;
        this.utilityUp = lnode.utilityUp;
        this.NumOfChildren = lnode.NumOfChildren;
        this.childrenArray.addAll(lnode.childrenArray);
        this.isPromising = lnode.isPromising;
        if (lnode.totalUtility == 0)
            this.gUtilityUp = lnode.utilityUp;
        else
            this.gUtilityUp = lnode.totalUtility;

        for (TreeNode node : lnode.children) {
            this.children.add(new GTreeNode(node));
        }
    }

    // add a child
    public void addTreeNode(GTreeNode treeNode) {
            children.add(treeNode);
            this.NumOfChildren++;
    }

    // update the node
    public void updateTreeNode(int currentUtility, int item, int index, int minUtility) {
//            int index = MapItemIndex.get(item);
//            int i =this.childrenArray[index];
//            int i = 0;
//            while (this.getChildren(i).item==item)
        this.getChildren(index).totalUtility += currentUtility;
        if (currentUtility >= minUtility) {
            this.getChildren(index).isPromising = true;
        }
    }

    //get the i-th child
    public GTreeNode getChildren(Integer i) {
        return children.get(i);
    }

    //if the item is the parent's child
    public int isChildExist(int item) {
//            if (item == -1){
//                return -1;
//            }
//            int index = MapItemIndex.get(item);
//            return this.childrenArray[index];
        return this.childrenArray.indexOf(item);
    }
}
