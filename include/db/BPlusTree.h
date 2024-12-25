
#ifndef __DB_BPlusTree_H__
#define __DB_BPlusTree_H__

#include <iostream>
#include <vector>
#include <string>
#include <stack>
#include <algorithm>
#include <cstring>
#include "datatype.h"
#define MAX_CHILDREN 48
#define MIN_CHILDREN 24

namespace db{

/*
b+树上的节点是TreeNode

b+树的一个节点上，需要记录父节点和子节点的相关信息（非叶子），叶子节点记录父节点和该block的信息


pkey，nextNode:该节点的键值，下一个节点。
对于非叶子节点：例如，根节点root现在size=3，root[0].nextNode表示 0号的Node 指向的子节点 满足 其最小键值是root[0].pkey
对于叶子节点：pkey就是插入的record的键值，nextNode置为nullptr，leaf[lastNode].nextNode=leafRight(用叶子节点的最后一个Node的nextNode指向其右边的节点)

blkid：只有是叶子节点才存有blockid的信息

大致结构如下：
                      (root)
                   [father=NULL]
    [pkey0]      |  [pkey1]        |      [pkey2]
    [child0]     |  [child1]       |      [child2]
________________________________________________________
   (child0)      |   (child1)      |   (child2)
[father=root]    |[father=root]    |[father=root]
[k0] [k1] [k2]   |[k0] [k1] [k2]   |[k0] [k1] [k2]
[c0] [c1] [c2]   |[c0] [c1] [c2]   |[c0] [c1] [c2]     
.......
.......
.......
.......
leaf TreeNode:
   [father]                         |   [father]                         |...
[k0] [k1] [k2]                      |[k0] [k1] [k2]                      |...
[]   []    [(指向其右边的节点)]     |[]   []    [(指向其右边的节点)]     |...
    [blkid]                         |    [blkid]                         |...
    

*/
struct TreeNode; 

struct Node {
    unsigned char* pkey;
    unsigned int len;
    TreeNode* nextNode;

    Node() : pkey(nullptr), len(0), nextNode(nullptr) {}

    Node(unsigned char* key, unsigned int len) : len(len), nextNode(nullptr) {
        pkey = new unsigned char[len];
        std::memcpy(pkey, key, len);
    }

    ~Node() {
        delete[] pkey;
    }

    // 添加拷贝构造函数
    Node(const Node& other) : len(other.len), nextNode(other.nextNode) {
        pkey = new unsigned char[len];
        std::memcpy(pkey, other.pkey, len);
    }

    // 添加拷贝赋值运算符
    Node& operator=(const Node& other) {
        if (this != &other) {
            delete[] pkey;
            len = other.len;
            nextNode = other.nextNode;
            pkey = new unsigned char[len];
            std::memcpy(pkey, other.pkey, len);
        }
        return *this;
    }
};

struct TreeNode {
    TreeNode* father;
    std::vector<Node> children;
    unsigned int blkid;

    TreeNode(unsigned int blkid = -1) : father(nullptr), blkid(blkid) {}

    ~TreeNode() {
        delete father;
    }
};


// table层在对bpt初始化时调用insert方法
// 迭代器：BlockIterator,RecordIterator
// 增删改查时同步更新bpt
class BPlusTree{
private:
    TreeNode* root;//根节点
   
    
private:    
    std::pair<bool,TreeNode*> find(TreeNode* start,unsigned char* pkey, unsigned int len);
    void insertLeafNode(TreeNode* newLeafNode);//叶子节点插入逻辑
    void insertInternalNode(TreeNode* parent, unsigned char* pkey, TreeNode* child);//内部节点插入逻辑
    void handleNodeUnderflow(TreeNode* node);//处理节点欠载
    

public:
    BPlusTree();
    ~BPlusTree();

    //向树中插入一条记录
    void insert(unsigned char* pkey, unsigned int len, unsigned int blkid);

    //从树中查找一个关键字序号为key的blkid
    unsigned int search(unsigned char* pkey, unsigned int len);

    //删除一条记录
    void remove(unsigned char* pkey, unsigned int len,unsigned int blkid);

    //更新一条记录的关键字
    void update(unsigned char* pkey, unsigned int len,unsigned int blkid);
};

}



#endif // __DB_BPlusTree_H__

#define __DB_BPlusTree_H__