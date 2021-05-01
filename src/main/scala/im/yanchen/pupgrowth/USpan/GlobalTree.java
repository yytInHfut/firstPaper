package im.yanchen.pupgrowth.USpan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Yuting Yang
 * @create 2021-04-23-23:02
 */
public class GlobalTree implements Serializable {
    long minUti;
    GTreeNode treeNode = new GTreeNode();
    List<int[]> candidate = new ArrayList<int[]>();
    candidateTreeNode candTree = new candidateTreeNode();
    public static int numOfHusps ;

    public GlobalTree(int uti, GTreeNode treeNode) {
        this.minUti = uti;
        this.treeNode = treeNode;
    }

    public GlobalTree(long minUti) {
        this.minUti = minUti;
    }

    public GlobalTree mergeTrees(GlobalTree gTree, Iterator<GTreeNode> trees) {
        treeNode = new GTreeNode(trees.hasNext() ? trees.next() : null);
        if (gTree.treeNode != null) {

            while (trees.hasNext()) {
                GTreeNode lTree = trees.next();
                mergeTwoTree(gTree.treeNode, lTree);
            }
        }

        return gTree;
    }

    public void mergeTwoTree(GTreeNode gTree, GTreeNode lTree) {
        ArrayList<Integer> gChildrenArray = gTree.childrenArray;
        ArrayList<Integer> lChildrenArray = lTree.childrenArray;

        //when one of the merge node is the leaf
        if (gChildrenArray.size() == 0 || lChildrenArray.size() == 0) {
            for (int i = 0; i < lChildrenArray.size(); i++) {
                int item = lChildrenArray.get(i);
                //just append the new node to the gTree
                gChildrenArray.add(item);
                gTree.addTreeNode(new GTreeNode(lTree.getChildren(i)));

            }
            int utilityUpper;
            if (gChildrenArray.size() == 0)
                utilityUpper = gTree.utilityUp;
            else
                utilityUpper = lTree.utilityUp;
            updateGUP(gTree, utilityUpper);
        } else {
            for (int i = 0; i < lChildrenArray.size(); i++) {
                int item = lChildrenArray.get(i);
                int gIndex;
                //just append the new node to the gTree
                if ((gIndex = gChildrenArray.indexOf(item)) == -1) {
                    gChildrenArray.add(item);
                    gTree.addTreeNode(new GTreeNode(lTree.getChildren(i)));
                }
                //update the same nodes between two trees
                else {
                    GTreeNode gNode = gTree.getChildren(gIndex);
                    GTreeNode lNode = lTree.getChildren(i);
                    if (item != -1) {
                        if (gNode.isPromising || lNode.isPromising) {
                            gNode.isPromising = true;

                        }
                        gNode.totalUtility += lNode.totalUtility;
                        gNode.utilityUp += lNode.utilityUp;
                        gNode.gUtilityUp += lNode.gUtilityUp;
                    }
                    mergeTwoTree(gNode, lNode);
                }
            }
        }
    }

    private void updateGUP(GTreeNode gNode, int utilityUpper) {
        for (GTreeNode node : gNode.children) {
            if (node.item != -1)
                node.gUtilityUp += utilityUpper;
            updateGUP(node, utilityUpper);
        }
    }

    public void printTree() {
        extracted(this.treeNode, new int[100], 0);
    }

    private void extracted(GTreeNode t, int[] prefix, int length) {
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
            if (t.isPromising && t.totalUtility >= minUti) {
                numOfHusps++;
                System.out.print("pattern: ");
                for (int i = 0; i <= length; i++) {
                    System.out.print(prefix[i] + " ");
                }

                System.out.println("  Utlity: " + t.totalUtility);
//                System.out.println("  UtlityUp: " + t.utilityUp);

            } else if (t.isPromising && t.gUtilityUp >= minUti) {
                System.out.print("Candidate: ");
                for (int i = 0; i <= length; i++) {
                    System.out.print(prefix[i] + " ");
                }

                System.out.println("  Utlity: " + t.totalUtility);
                System.out.println("  UtlityUp: " + t.gUtilityUp);
            }
            for (int i = 0; i < t.NumOfChildren; i++) {
                extracted(t.getChildren(i), prefix, length + 1);
            }
        }
    }

    public void getCandidates() {
        getCanditate(this.treeNode, new int[100], 0);
    }

    private void getCanditate(GTreeNode t, int[] prefix, int length) {
        if (t.item == 0) {
            for (int i = 0; i < t.NumOfChildren; i++) {
                getCanditate(t.getChildren(i), prefix, 0);
            }
        } else if (t.item == -1) {
            prefix[length] = -1;
            for (int i = 0; i < t.NumOfChildren; i++) {
                getCanditate(t.getChildren(i), prefix, length + 1);
            }
        } else {
            prefix[length] = t.item;
            if (t.isPromising && t.totalUtility < minUti && t.gUtilityUp >= minUti) {
                int[] cand = new int[length + 1];
                candidateTreeNode pNode = candTree;
                for (int i = 0; i <= length; i++) {
                    int index = pNode.childrenArray.indexOf(prefix[i]);
                    if (index == -1) {
                        candidateTreeNode node = new candidateTreeNode(prefix[i]);
                        if(i == length)
                            node.isPatternCand = true;
                        pNode.addNode(node);
                        pNode = node;
                    } else
                        pNode = pNode.children.get(index);
                    cand[i] = prefix[i];
                }
                candidate.add(cand);
            }
            for (int i = 0; i < t.NumOfChildren; i++) {
                getCanditate(t.getChildren(i), prefix, length + 1);
            }
        }
    }

    public candidateTreeNode getCandTree() {
        candidateTreeNode candidateTreeNode = new candidateTreeNode();
        return candidateTreeNode;
    }

    class candidateTreeNode  implements Serializable{
        public int item;
        public int utility;
        public boolean isPatternCand;

        public List<candidateTreeNode> children = new ArrayList<candidateTreeNode>();
        public ArrayList<Integer> childrenArray = new ArrayList<Integer>();

        public candidateTreeNode() {
            this.item = 0;
        }

        public candidateTreeNode(int item) {
            this.item = item;
        }

        public void addNode(candidateTreeNode node) {
            children.add(node);
            childrenArray.add(node.item);
        }

        public void printTree() {
            extracted(this, new int[100], 0);
        }

        private void extracted(candidateTreeNode t, int[] prefix, int length) {
            if (t.item == 0) {
                for (candidateTreeNode node : t.children) {
                    extracted(node, prefix, 0);
                }
            } else if (t.item == -1) {
                prefix[length] = -1;
                for (candidateTreeNode node : t.children) {
                    extracted(node, prefix, length + 1);
                }
            } else {
                prefix[length] = t.item;
                if(t.isPatternCand) {
                    System.out.print("Candidate: ");
                    for (int i = 0; i <= length; i++) {
                        System.out.print(prefix[i] + " ");
                    }

                    System.out.println("  Utlity: " + t.utility);
                }
                for (candidateTreeNode node : t.children) {
                    extracted(node, prefix, length + 1);
                }

            }
        }

//        public void  mergeCandTree(candidateTreeNode other){
//            merge(this, other);
//        }



    }


}
