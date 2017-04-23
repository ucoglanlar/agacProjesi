#include <iostream>
#include "BTree.h"
#include "BTreeNode.h"
#include "LeafNode.h"
#include "InternalNode.h"
using namespace std;

BTree::BTree(int ISize, int LSize):internalSize(ISize), leafSize(LSize)
{
  root = new LeafNode(LSize, NULL, NULL, NULL);
} // BTree::BTree()


void BTree::insert(const int value)
{
  // students must write this
	BTreeNode* result = root->insert(value);

	//Split and assign new root
	if(result != NULL){
	
		InternalNode* newRoot = new InternalNode(internalSize, leafSize, NULL, NULL, NULL);	
		
		//Check if the root split
		//Check may NOT be nessesary
		if(true){
			cout << endl << "Root split" << endl;
			newRoot->insert(root, result);	
		}
		
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
