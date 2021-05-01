package im.yanchen.pupgrowth.USpan;

import sun.reflect.generics.tree.Tree;

import java.io.Serializable;
import java.util.*;

/**
 * @author Yuting Yang
 * @create 2021-04-23-19:33
 */
public class TreeNode implements Serializable {
    // the node
    public int item;
    // the total utility in local partition
    public int totalUtility;
    public int utilityUp;
    public Object interInfo = null;
    //children info
    List<TreeNode> children = new ArrayList<TreeNode>();
    // -1/its number
    public ArrayList<Integer> childrenArray = new ArrayList<Integer>();
    //		public int[] childrenArray;
    public int NumOfChildren = 0;
    // whether is L_HUSP
    Boolean isPromising = false;

    // three kinds of constructors
    // the constructor of the root
    public TreeNode() {
        this.item = 0;
//            this.childrenArray = new int[20000];
//            Arrays.fill(childrenArray, -1);
    }

    // the constructor of item -1
    public TreeNode(int item) {
        this.item = -1;
//            this.childrenArray = new int[20000];
//            Arrays.fill(childrenArray, -1);
    }

    // the constructor of item
    public TreeNode(int item, int currentUtility, int utilUp) {
        this.item = item;
        this.totalUtility = currentUtility;
        this.utilityUp = utilUp;
//            this.childrenArray = new int[20000];
//            Arrays.fill(childrenArray, -1);
    }

    // add a child
    public void addTreeNode(TreeNode treeNode) {
        if (treeNode.item != -1) {
            children.add(treeNode);
            this.childrenArray.add(treeNode.item);
            this.NumOfChildren++;
        } else {
            children.add(treeNode);
            this.childrenArray.add(treeNode.item);
            this.NumOfChildren++;
        }
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
    public TreeNode getChildren(Integer i) {
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


    public void printTree() {
        extracted(this, new int[100], 0);
    }

    private void extracted(TreeNode t, int[] prefix, int length) {
        if (t.item == 0) {
            for (int i = 0; i < t.NumOfChildren; i++) {
                extracted(t.getChildren(i), prefix, 0);
            }
        } else if (t.item == -1) {
            prefix[length] = -1;
            for (int i = 0; i < t.NumOfChildren; i++) {
                extracted(t.getChildren(i), prefix, length + 1);
            }
        } else {
            prefix[length] = t.item;
            if (t.isPromising) {
                System.out.print("Candidate: ");
                for (int i = 0; i <= length; i++) {
                    System.out.print(prefix[i] + " ");
                }

                System.out.println("  Utlity: " + t.totalUtility);
                System.out.println("  UtlityUp: " + t.utilityUp);

            }
            for (int i = 0; i < t.NumOfChildren; i++) {
                extracted(t.getChildren(i), prefix, length + 1);
            }
        }
    }

    public void secondaryCompute(GlobalTree.candidateTreeNode cand){
        handleCandidate(this, cand, new int[100], 0);
    }

    private void handleCandidate(TreeNode thisNode, GlobalTree.candidateTreeNode cand, int[] prefix, int length){
        GlobalTree.candidateTreeNode cTempNode;
        TreeNode tTempNode;
        int cIndex;
        for (int i : thisNode.childrenArray){
            if( (cIndex = cand.childrenArray.indexOf(i)) != -1){
                tTempNode = thisNode.children.get(thisNode.childrenArray.indexOf(i));
                cTempNode = cand.children.get(cIndex);
                prefix[length] = i;

                if( i != -1) {
                    if (cTempNode.isPatternCand) {
                        cTempNode.utility += tTempNode.totalUtility;
                    }

                    if(tTempNode.totalUtility != 0){
                        handleCandidate(tTempNode, cTempNode, prefix, length + 1);
                    }

                    if(tTempNode.interInfo != null){
                        secondaryMining(cTempNode, (List<QMatrixProjection>) tTempNode.interInfo, prefix, length + 1);
                    }
                }
                else {
                    handleCandidate(tTempNode, cTempNode, prefix, length + 1);
                }

            }
        }
    }

    private void secondaryMining(GlobalTree.candidateTreeNode cTempNode, List<QMatrixProjection> interInfo, int[] prefix, int length) {
        // For each item
        for (int item : cTempNode.childrenArray) {
            prefix[length] = item;
            int cIndex = cTempNode.children.indexOf(item);
            GlobalTree.candidateTreeNode tempNode = cTempNode.children.get(cIndex);

            // This variable will be used to calculate this item's utility for the whole database
            int totalUtility = 0;
            // This variable will be used to calculate this item's remaining utility for the whole database
            int totalRemainingUtility = 0;

            // Initialize a variable to store the projected QMatrixes for the i-concatenation
            // of this item to the prefix
            List<QMatrixProjection> matrixProjections = new ArrayList<QMatrixProjection>();

            if(item != -1) {
                // for each sequence in the projected database
                for (QMatrixProjection qmatrix : interInfo) {

                    // if the item appear in that sequence
                    int rowItem = Arrays.binarySearch(qmatrix.getItemNames(), item);
                    if (rowItem >= 0) {

                        // We initialize two variables that will be used to calculate the maximum
                        // utility and remaining utility for the i-concatenation with this item
                        // in that sequence
                        int maxUtility = 0;
                        int maxRemainingUtility = 0;

                        // create a list to store the matrix positions of i-concatenations with
                        // this item in that sequence
                        List<MatrixPosition> positions = new ArrayList<MatrixPosition>();

                        // for each position of the prefix
                        for (MatrixPosition position : qmatrix.positions) {
                            // We will look for this item in the same column (in the same itemset)
                            // because we look for a i-concatenation
                            int column = position.column;

                            // we will check if the new item appears in the same itemset
                            int newItemUtility = qmatrix.getItemUtility(rowItem, column);
                            // if the item appears in that itemset
                            if (newItemUtility > 0) {
                                // calculate the utility of the i-concatenation at this position
                                // in that sequence
                                int newPrefixUtility = position.utility + newItemUtility;
                                // Add this new position and its utility in the list of position
                                // for this pattern
                                positions.add(new MatrixPosition(rowItem, column, newPrefixUtility));

                                // If the utility of this new i-concatenation is higher than
                                // previous occurrences of that same pattern
                                if (newPrefixUtility > maxUtility) {
                                    // record this utility as the maximum utility until now for that pattern
                                    maxUtility = newPrefixUtility;

                                    // Get the remaining utility at that position
                                    int remaining = qmatrix.getRemainingUtility(rowItem, column);

                                    // If it is the first position where this i-concatenation occurs
                                    // we record its remaining utility as the largest remaining utility
                                    // for this i-concatenation
                                    if (remaining > 0 && maxRemainingUtility == 0) {
                                        maxRemainingUtility = remaining;
                                    }
                                }
                            }

                        }

                        // update the total utility and total remaining utility for that i-concatenation
                        // for all sequences by adding the utility and remaining utility for the
                        // current sequence
                        totalUtility += maxUtility;
                        totalRemainingUtility += maxRemainingUtility;

                        // create the projected matrix for the current sequence
                        QMatrixProjection projection = new QMatrixProjection(qmatrix, positions);
                        // Add it to the projected database for that i-concatenation.
                        matrixProjections.add(projection);
                    }
                }

                // create the i-concatenation by appending the item to the prefix in the buffer
                prefix[length] = item;
                tempNode.utility = totalUtility;
                if(tempNode.childrenArray.size() > 0){
                    secondaryMining(tempNode, matrixProjections, prefix, length + 1);
                }
            }
            else {
                secondaryMining(tempNode, matrixProjections, prefix, length + 1);
            }

        }
    }

}
