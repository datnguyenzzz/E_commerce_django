import os

TOP_POPULAR_SIZE = int(os.getenv("TOP_POPULAR_SIZE"))

class Node:
    def __init__(self):
        #["char" : Node()]
        self.children = dict() 
        self.top_popular = set()
    
    def __str__(self):
        return f'Children: {self.children}; top_pick: {self.top_popular}'
    
class Trie:
    def __init__(self):
        self._root = Node() 
    
    @property
    def root(self):
        return self._root
    
    def view(self, pa, u):
        print(u)
        for v in u.children.values():
            if v==pa:
                continue 
            self.view(u,v)
    
    def add_word(self, word):
        word = word.lower() 
        curr = self._root 
        for ch in word:
            if ch in curr.children:
                curr = curr.children.get(ch) 
            else:
                n_curr = Node() 
                curr.children[ch] = n_curr
                curr = n_curr 
            
            if len(curr.top_popular) < TOP_POPULAR_SIZE:
                curr.top_popular.add(word)
    
    def get_top_popular(self, prefix):
        prefix = prefix.lower() 
        curr = self._root
        
        for ch in prefix:
            if ch not in curr.children:
                return set() 
            
            curr = curr.children[ch]
        
        return list(curr.top_popular)
    
if __name__ == "__main__":
    trie = Trie() 
    trie.add_word("abc")
    trie.add_word("aac")
    trie.add_word("aab")
    trie.add_word("abd")
    trie.add_word("acd")
    trie.add_word("ace")
    trie.add_word("acc")    
    trie.view(trie.root, trie.root)
    print(trie.get_top_popular("ab"))
    