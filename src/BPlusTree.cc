#include<db/BPlusTree.h>


namespace db{


BPlusTree::BPlusTree(){
}

BPlusTree::~BPlusTree(){  
}


void BPlusTree::insert(unsigned char* pkey, unsigned int len, unsigned int blkid) {
    Node newNode(pkey,len);

    if(root->children.empty()){//根节点的子节点信息为空，初始化根节点
        root->children.push_back(newNode);
        return;
    }
    
    std::pair<bool,TreeNode*> resultFind=find(root,pkey,len);
    
    if(!resultFind.first){//没有这个pkey才需要插入
        if(resultFind.second->blkid==blkid){//blkid存在
            resultFind.second->children.push_back(newNode);
            return;
        }
        //blkid不存在
        TreeNode* newLeafNode=new TreeNode(blkid);
        newLeafNode->children.push_back(newNode);
        insertLeafNode(newLeafNode);
    }
}

void BPlusTree::insertLeafNode(TreeNode* newLeafNode) {
    // 找到需要插入的位置，从根节点开始
    TreeNode* current = root;
    
    // 用于回溯路径的栈
    std::stack<TreeNode*> path;
    
    while (!current->children.empty() && current->children[0].nextNode != nullptr) {
        path.push(current);
        
        // 寻找正确的子节点
        unsigned int i = 0;
        while (i < current->children.size() && 
               std::memcmp(current->children[i].pkey, newLeafNode->children[0].pkey, newLeafNode->children[0].len) < 0) {
            i++;
        }
        
        if (i > 0 && std::memcmp(current->children[i - 1].pkey, newLeafNode->children[0].pkey, newLeafNode->children[0].len) == 0) {
            i--;
        }
        
        current = current->children[i].nextNode;
    }
    
    // 找到当前叶子节点的位置，插入新的节点
    unsigned int i = 0;
    while (i < current->children.size() && 
           std::memcmp(current->children[i].pkey, newLeafNode->children[0].pkey, newLeafNode->children[0].len) < 0) {
        i++;
    }
    
    current->children.insert(current->children.begin() + i, newLeafNode->children[0]);
    
    // 检查是否需要分裂叶子节点
    if (current->children.size() > MAX_CHILDREN) {
        TreeNode* newSibling = new TreeNode();
        unsigned int mid = current->children.size() / 2;
        
        // 将后半部分移动到新节点
        newSibling->children.assign(current->children.begin() + mid, current->children.end());
        current->children.erase(current->children.begin() + mid, current->children.end());
        
        // 更新父节点的信息
        newSibling->father = current->father;
        if (current->father == nullptr) {
            // 如果当前节点是根节点，创建新的根节点
            TreeNode* newRoot = new TreeNode();
            newRoot->children.push_back(Node(current->children[0].pkey, current->children[0].len));
            newRoot->children[0].nextNode = current;
            newRoot->children.push_back(Node(newSibling->children[0].pkey, newSibling->children[0].len));
            newRoot->children[1].nextNode = newSibling;
            current->father = newRoot;
            newSibling->father = newRoot;
            root = newRoot;
        } else {
            // 插入父节点
            path.push(current);
            current = current->father;
            
            unsigned char* midKey = newSibling->children[0].pkey;
            insertInternalNode(current, midKey, newSibling);
        }
    }
}

void BPlusTree::insertInternalNode(TreeNode* parent, unsigned char* pkey, TreeNode* child) {
    unsigned int i = 0;
    while (i < parent->children.size() && 
           std::memcmp(parent->children[i].pkey, pkey, parent->children[i].len) < 0) {
        i++;
    }
    
    parent->children.insert(parent->children.begin() + i, Node(pkey, child->children[0].len));
    parent->children[i].nextNode = child;
    
    // 检查是否需要分裂内部节点
    if (parent->children.size() > MAX_CHILDREN) {
        TreeNode* newSibling = new TreeNode();
        unsigned int mid = parent->children.size() / 2;
        
        // 将后半部分移动到新节点
        newSibling->children.assign(parent->children.begin() + mid, parent->children.end());
        parent->children.erase(parent->children.begin() + mid, parent->children.end());
        
        // 更新父节点的信息
        newSibling->father = parent->father;
        if (parent->father == nullptr) {
            // 如果当前节点是根节点，创建新的根节点
            TreeNode* newRoot = new TreeNode();
            newRoot->children.push_back(Node(parent->children[0].pkey, parent->children[0].len));
            newRoot->children[0].nextNode = parent;
            newRoot->children.push_back(Node(newSibling->children[0].pkey, newSibling->children[0].len));
            newRoot->children[1].nextNode = newSibling;
            parent->father = newRoot;
            newSibling->father = newRoot;
            root = newRoot;
        } else {
            // 插入父节点
            insertInternalNode(parent->father, newSibling->children[0].pkey, newSibling);
        }
    }
}




unsigned int BPlusTree::search(unsigned char* pkey, unsigned int len){
    return find(root,pkey,len).second->blkid;
}

std::pair<bool,TreeNode*> BPlusTree::find(TreeNode* start,unsigned char* pkey, unsigned int len) { 
    DataType::Less less;//比较器

    std::vector<Node>::iterator prev;//取出节点中的一条数据
    prev = start->children.begin();

    /*
    遍历这个节点的内容
    1.先找到满足 pkey <= it.pkey 的位置
    2.对于非叶子节点(nextNode不空)：
    prev.pkey < pkey < it.pkey ：find(prev.nextNode)
    pkey = it.pkey : find(it.nextNode)
    3.对于叶子节点(nextNode空)：
    prev.pkey < pkey < it.pkey ：没有这样的关键字，返回(false,start)
    pkey = it.pkey : 要找的关键字就是当前关键字，返回(true,start)
    */
    for (std::vector<Node>::iterator it = start->children.begin(); it != start->children.end(); ++it) {
        bool ret = less(it->pkey,it->len,pkey,len);
        if(ret){//pkey > it.pkey
            prev = it;
            continue;
        }


        //pkey <= it.pkey
        bool ret2 = less(pkey,len,it->pkey,it->len);
        if(ret2){//pkey < it.pkey
            if(prev->nextNode!=nullptr) {
                return find(prev->nextNode,pkey,len);
            }
            return std::pair<bool,TreeNode*>(false,start); 
        }
            //(else)pkey = it.pkey
        if(it->nextNode!=nullptr){
            return find(it->nextNode,pkey,len);
        };
        return std::pair<bool,TreeNode*>(true,start);
    }
}




void BPlusTree::remove(unsigned char* pkey, unsigned int len, unsigned int blkid) {
    std::pair<bool, TreeNode*> result = find(root, pkey, len);
    TreeNode* leafNode = result.second;
    
    // 找到目标节点
    auto it = std::find_if(leafNode->children.begin(), leafNode->children.end(),
                           [&](const Node& node) {
                               return std::memcmp(node.pkey, pkey, len) == 0;
                           });
    
    if (it != leafNode->children.end()) {
        // 移除目标节点
        leafNode->children.erase(it);
        
        // 检查是否需要合并或借用节点
        if (leafNode->children.size() < MIN_CHILDREN) {
            handleNodeUnderflow(leafNode);
        }
    }
}

void BPlusTree::handleNodeUnderflow(TreeNode* node) {
    // 如果是根节点，特殊处理
    if (node == root) {
        if (root->children.size() == 0) {
            if (root->children[0].nextNode != nullptr) {
                root = root->children[0].nextNode;
                root->father = nullptr;
            }
        }
        return;
    }
    
    TreeNode* parent = node->father;
    auto it = std::find_if(parent->children.begin(), parent->children.end(),
                           [&](const Node& child) {
                               return child.nextNode == node;
                           });
    int index = std::distance(parent->children.begin(), it);
    
    TreeNode* leftSibling = (index > 0) ? parent->children[index - 1].nextNode : nullptr;
    TreeNode* rightSibling = (index < parent->children.size() - 1) ? parent->children[index + 1].nextNode : nullptr;
    
    if (leftSibling && leftSibling->children.size() > MIN_CHILDREN) {
        // 从左兄弟借一个节点
        node->children.insert(node->children.begin(), leftSibling->children.back());
        leftSibling->children.pop_back();
        parent->children[index].pkey = node->children[0].pkey;
    } else if (rightSibling && rightSibling->children.size() > MIN_CHILDREN) {
        // 从右兄弟借一个节点
        node->children.push_back(rightSibling->children.front());
        rightSibling->children.erase(rightSibling->children.begin());
        parent->children[index + 1].pkey = rightSibling->children[0].pkey;
    } else {
        // 合并节点
        if (leftSibling) {
            leftSibling->children.insert(leftSibling->children.end(), node->children.begin(), node->children.end());
            parent->children.erase(parent->children.begin() + index);
            delete node;
        } else if (rightSibling) {
            node->children.insert(node->children.end(), rightSibling->children.begin(), rightSibling->children.end());
            parent->children.erase(parent->children.begin() + index + 1);
            delete rightSibling;
        }
        
        if (parent->children.size() < MIN_CHILDREN) {
            handleNodeUnderflow(parent);
        }
    }
}




void BPlusTree::update(unsigned char* pkey, unsigned int len, unsigned int blkid) {
    std::pair<bool, TreeNode*> result = find(root, pkey, len);
    TreeNode* leafNode = result.second;
    
    // 找到目标节点并更新
    auto it = std::find_if(leafNode->children.begin(), leafNode->children.end(),
                           [&](const Node& node) {
                               return std::memcmp(node.pkey, pkey, len) == 0;
                           });
    
    if (it != leafNode->children.end()) {
        it->nextNode->blkid = blkid;
    } else {
        std::cerr << "Key not found, cannot update." << std::endl;
    }
}




}