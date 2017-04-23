#include <iostream>
#include "BTree.h"
#include "BTreeNode.h"
#include "LeafNode.h"
#include "InternalNode.h"
using namespace std;

BTreeNode* InternalNode::newRoot = NULL;

BTree::BTree(int ISize, int LSize):internalSize(ISize), leafSize(LSize)
{
  root = new LeafNode(LSize, NULL, NULL, NULL);
} // BTree::BTree()


void BTree::insert(const int value)
{
  // students must write this
  
	BTreeNode* result;
	BTreeNode* leaf;
	LeafNode* rootLeaf = dynamic_cast<LeafNode*>(root);
	InternalNode* rootInternal = dynamic_cast<InternalNode*>(root);
	
	rootInternal->newRoot = 0;
	
	//Root is a leaf, insert to leaf
	if(rootLeaf != 0){
		
		//cout << "Root is leaf" << endl;
		result = rootLeaf->insert(value);
		
	
	//Root is a internal node, 
	}else{
		//cout << "Root is internal" << endl;
		leaf = rootInternal->find(value, root);
		result = leaf->insert(value);
		
		result = rootInternal->newRoot;
		
		 
		
		//cout << "New Root: " << rootInternal->newRoot << endl;
	}
	
	

	//Split and assign new root
	if(result != NULL){
	
		InternalNode* newRoot = new InternalNode(internalSize, leafSize, NULL, NULL, NULL);	
		
		//cout << endl << "Root split" << endl;
		
		//cout << "Root count: " << root->getCount() << endl;
		//cout << "Result count: " << result->getCount() << endl;
		
		newRoot->insert(root, result);	
	
		
		//Set old root to be equal to the new root
		root = newRoot;
	
	}

} // BTree::insert()


void BTree::print()
{
	
  BTreeNode *BTreeNodePtr;
  Queue<BTreeNode*> queue(1000);

  queue.enqueue(root);
  while(!queue.isEmpty())
  {
    BTreeNodePtr = queue.dequeue();
    
    BTreeNodePtr->print(queue);
  } // while
} // BTree::print()
