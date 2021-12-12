import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

public class Trie {

    private final static int TOP_POPULAR_SIZE = 5;

    public static class Node {
        private HashMap<Character, Node> children;
        private ArrayList<String> topPopular;

        public Node() {
            this.children = new HashMap<Character, Node>(); 
            this.topPopular = new ArrayList<String>();
        }

        public HashMap<Character, Node> getChildren() {
            return this.children;
        }

        public void addChild(Character ch, Node node) {
            this.children.put(ch, node);
        }

        public ArrayList<String> getTopPopular() {
            return this.topPopular;
        }

        public void addTopPopular(String word) {
            this.topPopular.add(word);
        }

        @Override
        public String toString() {
            return "Children = " + this.children.size()+ "; "
                + "Top popular = [ " + this.topPopular.toString() + "]\n";
        }
    }

    public static class TrieBuilder {
        private Node root;

        public TrieBuilder() {
            this.root = new Node();
        }

        public Node getRoot() {
            return this.root;
        }

        private void view(Node pa, Node u) {
            System.out.println(u);
            for (Map.Entry<Character, Node> e: u.getChildren().entrySet()) {
                Node v = (Node) e.getValue();
                if (v==pa) {
                    continue;
                }

                this.view(u,v);
            }
        }

        public void addWord(String word) {
            word = word.toLowerCase();
            Node curr = this.root;
            for (char ch: word.toCharArray()) {
                if (curr.getChildren().get(ch) != null) {
                    curr = curr.getChildren().get(ch);
                } else {
                    Node n_curr = new Node(); 
                    curr.addChild(ch, n_curr);
                    curr = n_curr;
                }

                if (curr.getTopPopular().size() < TOP_POPULAR_SIZE) {
                    curr.addTopPopular(word);
                }
            }
        }

        public ArrayList<String> getTopPopular(String prefix) {
            prefix = prefix.toLowerCase();
            Node curr = this.root;

            for (char ch: prefix.toCharArray()) {
                if (curr.getChildren().get(ch) == null) {
                    return new ArrayList<String>();
                }

                curr = curr.getChildren().get(ch);
                //System.out.println(curr);
            }
            
            return curr.getTopPopular();
        }
    }

    public static void main(String[] args) {
    }
}